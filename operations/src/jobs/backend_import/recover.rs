//! Registers a file whose version never committed as the key and version its write intent or
//! its path names.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::path::Path;
use std::sync::Arc;

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::keyspaces::{BLOB_VERSIONS_KEYSPACE, IMPORTED_OBJECT_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::rows::values;
use aruna_core::structs::storage::backend_import::{ExternalObject, ImportedObject, object_key};
use aruna_core::structs::storage::blob::{ResolvedBackend, ensure_confined_path};
use aruna_core::types::{Key, Value};
use ulid::Ulid;

use super::reads::{
    ClaimsOperation, HideAliasOperation, StoredIntentsOperation, exact_of, hash_call, head_call,
    head_of, listed_hash, note_call, noted, read_call, version_row,
};
use super::{
    Claim, Exact, Fence, Hashed, Identity, ImportJob, ImportRun, ItemError, Outcome, choose_identity,
    claimed, complete_call, encode, item, moved_away, observed, put_failure, put_operation,
    rule_guard, shared_guards,
};
use crate::jobs::import_listing::ListingNote;
use crate::s3::object::put::{PutObjectError, PutObjectResult};

#[derive(Debug, PartialEq)]
enum Step {
    Claims,
    FirstHash(Vec<Claim>),
    LateHash,
    Exact,
    Head,
    Retired(Option<Ulid>),
    Put { keep_head: bool, size: u64 },
    Complete,
    ExactAgain,
    Hide,
    NoteRetired(Value),
    Note,
}

type Output = Option<Outcome>;
type Turn = Result<Next<Output>, ItemError>;

/// Decides whether a file is an uncommitted write and registers it. `None` leaves the file to
/// the ordinary import, as for any row Aruna wrote.
#[derive(Debug, PartialEq)]
pub(super) struct RecoverOperation {
    job: ImportJob,
    run: Arc<ImportRun>,
    object: ExternalObject,
    row: Option<ImportedObject>,
    stored: Option<Value>,
    resolved: ResolvedBackend,
    hashed: Option<Hashed>,
    identity: Option<Identity>,
    committed: Option<ImportedObject>,
    version_key: Option<Key>,
    exact_value: Option<Value>,
    step: Step,
}

impl RecoverOperation {
    pub(super) fn new(
        job: ImportJob,
        run: Arc<ImportRun>,
        object: ExternalObject,
        (row, stored): (Option<ImportedObject>, Option<Value>),
        resolved: ResolvedBackend,
    ) -> Self {
        Self {
            job,
            run,
            object,
            row,
            stored,
            resolved,
            hashed: None,
            identity: None,
            committed: None,
            version_key: None,
            exact_value: None,
            step: Step::Claims,
        }
    }

    fn hash_call(&self) -> IoCall {
        hash_call(&self.job, self.resolved.clone(), &self.object)
    }

    fn lost() -> ItemError {
        ItemError::System("the recovery lost its state".to_string())
    }

    fn identity(&self) -> Result<&Identity, ItemError> {
        self.identity.as_ref().ok_or_else(Self::lost)
    }

    fn with_claims(&mut self, claims: Vec<Claim>) -> Turn {
        if super::row_covers(self.row.as_ref(), &claims) {
            return Ok(Next::Done(None));
        }
        // Only the hash of the file can prove which write left it, so it is read first.
        if claims.is_empty() {
            return self.choose(claims);
        }
        self.step = Step::FirstHash(claims);
        Ok(Next::Call(self.hash_call()))
    }

    fn choose(&mut self, claims: Vec<Claim>) -> Turn {
        let run = self.run.clone();
        let hash = (self.hashed.as_ref())
            .and_then(|(location, ..)| location.get_blake3())
            .and_then(|hash| <[u8; 32]>::try_from(hash).ok());
        let names = (&run.rule, run.earlier.as_slice(), run.discovery.as_slice());
        let Some(identity) = choose_identity(names, &self.object.key, claims, hash.as_ref())?
        else {
            return Ok(Next::Done(None));
        };
        ensure_confined_path(Path::new(&identity.key))
            .map_err(|error| item(error.to_string()))?;
        // A name alone proves no owner where several rules read the folder.
        if identity.intent.is_none()
            && self.row.is_none()
            && let Some(outcome) = claimed(&run, &self.object.key)?
        {
            return Ok(Next::Done(Some(outcome)));
        }
        self.identity = Some(identity);
        if self.hashed.is_some() {
            return self.with_hash();
        }
        self.step = Step::LateHash;
        Ok(Next::Call(self.hash_call()))
    }

    /// The file's bytes are known: looks up the version its identity names.
    fn with_hash(&mut self) -> Turn {
        let identity = self.identity()?;
        let (key, version_id) = (identity.key.clone(), identity.version_id);
        let writer = identity.intent.as_ref().map(|(_, writer)| *writer);
        let hashed = self.hashed.as_mut().ok_or_else(Self::lost)?;
        if let Some(writer) = writer {
            hashed.0.created_by = writer;
        }
        let object = observed(&self.object, hashed.2);
        self.committed = Some(ImportedObject {
            key: Some(key.clone()),
            ..ImportedObject::new(version_id, &object)
        });
        self.object = object;
        let row = version_row(&self.run.rule.bucket, &key, version_id)?;
        self.version_key = Some(row.clone());
        self.step = Step::Exact;
        Ok(Next::Call(read_call([(BLOB_VERSIONS_KEYSPACE, row)])))
    }

    /// The row of a literal-name import of the file and its literal key, once the file has
    /// another key; it stops being current only once its replacement exists.
    fn alias(&self) -> Option<(Ulid, String)> {
        let identity = self.identity.as_ref()?;
        let literal = (self.run.rule.aruna_key(&self.object.key))
            .filter(|literal| *literal != identity.key)?;
        let row = self.row.as_ref().filter(|row| row.key.is_none())?;
        Some((row.version_id, literal))
    }

    fn with_exact(&mut self, exact: Exact) -> Turn {
        let identity = self.identity()?;
        let (key, version_id) = (identity.key.clone(), identity.version_id);
        let hashed = self.hashed.as_ref().ok_or_else(Self::lost)?;
        match &exact {
            Exact::Incompatible => {
                return Err(item(format!(
                    "version {version_id} of {key} exists in a state that cannot hold the file"
                )));
            }
            Exact::Materialized(hash, _) if hashed.0.get_blake3() != Some(&hash[..]) => {
                return Err(item(format!(
                    "the file differs from version {version_id} of {key}"
                )));
            }
            Exact::Absent | Exact::Materialized(..) => {}
        }
        match exact {
            Exact::Absent => {
                // The version transaction reads the delete trail again, so a delete meanwhile
                // conflicts.
                self.step = Step::Head;
                Ok(Next::Call(head_call(&self.run.rule.bucket, &key)?))
            }
            exact => self.after_version(exact),
        }
    }

    /// The version exists: a literal alias is hidden, then its missing row is written.
    fn after_version(&mut self, exact: Exact) -> Turn {
        let Exact::Materialized(_, value) = exact else {
            // An existing version only gets its missing row, and only for the same bytes.
            return self.hide_or(None);
        };
        self.hide_or(Some(value))
    }

    fn hide_or(&mut self, value: Option<Value>) -> Turn {
        match self.alias() {
            Some((version_id, literal)) => {
                self.step = Step::Hide;
                self.exact_value = value;
                let rule = self.run.rule.clone();
                let hide = HideAliasOperation::new(self.job.clone(), rule, literal, version_id);
                Ok(Next::Call(IoCall::child(hide)))
            }
            None => self.note_row(value),
        }
    }

    fn note_row(&mut self, value: Option<Value>) -> Turn {
        let Some(value) = value else {
            return Ok(Next::Done(Some(Outcome::Skipped)));
        };
        let identity = self.identity()?;
        if identity.intent.is_some() {
            let intents = StoredIntentsOperation::new(
                self.run.rule.import_id,
                &self.object.key,
                identity.version_id,
            );
            self.step = Step::NoteRetired(value);
            return Ok(Next::Call(IoCall::child(intents)));
        }
        self.write_note(value, Vec::new())
    }

    fn write_note(&mut self, value: Value, retired: Vec<Key>) -> Turn {
        let rule = &self.run.rule;
        let version_key = self.version_key.clone().ok_or_else(Self::lost)?;
        let committed = self.committed.as_ref().ok_or_else(Self::lost)?;
        let note = ListingNote {
            guards: vec![
                rule_guard(rule)?,
                (BLOB_VERSIONS_KEYSPACE.to_string(), version_key, Some(value)),
            ],
            retired,
            ..ListingNote::default()
        };
        let row_key = object_key(rule.import_id, &self.object.key);
        let repaired = encode(committed)?;
        self.step = Step::Note;
        Ok(Next::Call(note_call(
            rule,
            row_key,
            self.stored.clone(),
            repaired,
            note,
        )))
    }

    /// The version is absent: retires the write's intents with it, unless an alias stays.
    fn with_head(&mut self, head: Option<Ulid>) -> Turn {
        let identity = self.identity()?;
        if identity.intent.is_some() && self.alias().is_none() {
            let intents = StoredIntentsOperation::new(
                self.run.rule.import_id,
                &self.object.key,
                identity.version_id,
            );
            self.step = Step::Retired(head);
            return Ok(Next::Call(IoCall::child(intents)));
        }
        self.put(head, Vec::new())
    }

    fn put(&mut self, head: Option<Ulid>, retired: Vec<Key>) -> Turn {
        let run = self.run.clone();
        let rule = &run.rule;
        let identity = self.identity()?;
        let (key, version_id) = (identity.key.clone(), identity.version_id);
        let mut guards = vec![
            rule_guard(rule)?,
            (
                IMPORTED_OBJECT_KEYSPACE.to_string(),
                object_key(rule.import_id, &self.object.key),
                self.stored.clone(),
            ),
        ];
        if self.row.is_none() {
            guards.extend(shared_guards(&run, &self.object.key));
        }
        // Older history joins below a head; a newer file becomes the head.
        let keep_head = head.is_some_and(|head| head > version_id);
        let fence = Fence {
            head,
            keep_head,
            guards,
            retired,
            trail: Some(version_id),
            // An alias keeps its row until the alias is hidden.
            keep_row: self.alias().is_some(),
            copy_index: None,
        };
        let (location, version, _) = self.hashed.clone().ok_or_else(Self::lost)?;
        let size = location.blob_size;
        let committed = self.committed.clone().ok_or_else(Self::lost)?;
        let file = (location, version);
        let put = put_operation((&self.job, &run), key, file, committed, fence);
        self.step = Step::Put { keep_head, size };
        match put {
            Ok(put) => Ok(Next::Call(IoCall::child(put))),
            Err(error) => self.put_ended(Err(error), keep_head, size),
        }
    }

    fn put_ended(
        &mut self,
        put: Result<PutObjectResult, PutObjectError>,
        keep_head: bool,
        size: u64,
    ) -> Turn {
        match put {
            Ok(_) => {}
            // Another write or recovery came first; the next run looks again.
            Err(PutObjectError::HeadChanged | PutObjectError::VersionExists) => {
                return Ok(Next::Done(Some(Outcome::Skipped)));
            }
            Err(error) if moved_away(&error) => return Ok(Next::Done(Some(Outcome::Ignored))),
            Err(error) => return Err(put_failure(error)),
        }
        if keep_head {
            return self.after_put();
        }
        let identity = self.identity()?;
        let (key, version_id) = (identity.key.clone(), identity.version_id);
        let run = self.run.clone();
        self.step = Step::Complete;
        Ok(Next::Call(complete_call(
            (&self.job, &run),
            key,
            version_id,
            size,
        )))
    }

    fn after_put(&mut self) -> Turn {
        if self.alias().is_none() {
            return Ok(Next::Done(Some(Outcome::Imported)));
        }
        let row = self.version_key.clone().ok_or_else(Self::lost)?;
        self.step = Step::ExactAgain;
        Ok(Next::Call(read_call([(BLOB_VERSIONS_KEYSPACE, row)])))
    }
}

impl Operation for RecoverOperation {
    type Output = Output;
    type Error = ItemError;

    fn first(&mut self) -> Turn {
        let intents = (self.run.intents.get(&self.object.key)).cloned().unwrap_or_default();
        if intents.is_empty() {
            return self.with_claims(Vec::new());
        }
        let claims = ClaimsOperation::new(&self.run.rule, intents);
        Ok(Next::Call(IoCall::child(claims)))
    }

    fn next(&mut self, result: IoResult) -> Turn {
        match std::mem::replace(&mut self.step, Step::Complete) {
            Step::Claims => {
                let claims = result.child::<Vec<Claim>, ItemError>()??;
                self.with_claims(claims)
            }
            Step::FirstHash(claims) => {
                self.hashed = Some(listed_hash(result, &self.object)?);
                self.choose(claims)
            }
            Step::LateHash => {
                self.hashed = Some(listed_hash(result, &self.object)?);
                self.with_hash()
            }
            Step::Exact => {
                let [value] = values::<1>(result)?;
                self.with_exact(exact_of(value)?)
            }
            Step::Head => {
                let head = head_of(result)?;
                self.with_head(head)
            }
            Step::Retired(head) => {
                let retired = result.child::<Vec<Key>, ItemError>()??;
                self.put(head, retired)
            }
            Step::Put { keep_head, size } => {
                let put = result.child::<PutObjectResult, PutObjectError>()?;
                self.put_ended(put, keep_head, size)
            }
            Step::Complete => self.after_put(),
            Step::ExactAgain => {
                let [value] = values::<1>(result)?;
                self.after_version(exact_of(value)?)
            }
            Step::Hide => {
                result.child::<(), ItemError>()??;
                let value = self.exact_value.take();
                self.note_row(value)
            }
            Step::NoteRetired(value) => {
                let retired = result.child::<Vec<Key>, ItemError>()??;
                self.write_note(value, retired)
            }
            Step::Note => Ok(Next::Done(Some(match noted(result)? {
                true => Outcome::Imported,
                false => Outcome::Skipped,
            }))),
        }
    }
}
