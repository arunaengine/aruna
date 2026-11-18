//! Decides the grant of a finished export: the stored grant while it is valid, or a new one
//! signed from the finished export checkpoint. Nothing is written here.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::document::DocumentTarget;
use aruna_core::effects::{BlobEffect, Effect};
use aruna_core::errors::BlobError;
use aruna_core::events::{BlobEvent, Event};
use aruna_core::federation::Signed;
use aruna_core::keyspaces::{
    BLOB_VERSIONS_KEYSPACE, BUCKET_ENCRYPTION_KEYSPACE, FEDERATION_KEYSPACE, JOB_STATE_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::rows::{decode, decoded, read_row, values};
use aruna_core::structs::execution::job::{ExportSelection, JobId};
use aruna_core::structs::identity::auth::{AuthContext, NodeCapabilities, Permission};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::storage::blob::{BlobVersion, VersionKey};
use aruna_core::structs::storage::encryption::{BucketEncryption, BucketKeyRef};
use aruna_core::transfer::{
    ExportGrant, GrantError, GrantRecord, MAX_TRANSFER_SECS, PinnedSource, check_issued,
    selection_digest,
};
use ulid::Ulid;
use url::Url;

use crate::auth::request_policy::PolicyRequestExtras;
use crate::federation::access::{AccessCheckOperation, access_granted};
use crate::federation::records::{RecordChange, RecordError, RecordOutcome, change_call};
use crate::jobs::export::ExportCheckpoint;
use crate::replication::plaintext::{HolderCheckOperation, PlaintextError};

const EXPORT_OPERATION: &str = "federation.export";

pub fn record_key(job_id: JobId) -> Vec<u8> {
    [&b"grant/"[..], &job_id.to_bytes()].concat()
}

/// Owned inputs of [`IssueGrantOperation`].
#[derive(Clone, Debug, PartialEq)]
pub struct IssueGrantConfig {
    pub realm_id: RealmId,
    pub principal: UserId,
    pub job_id: JobId,
    pub document_id: Ulid,
    pub document_path: String,
    pub selection: ExportSelection,
    pub artifact_url: Url,
    pub capabilities: NodeCapabilities,
    pub now: u64,
}

/// Reads the grant record and checkpoint of an export, then returns the stored grant while it
/// is valid, or signs a new one from the finished checkpoint. Nothing is written here.
#[derive(Debug, PartialEq)]
pub struct IssueGrantOperation {
    config: IssueGrantConfig,
}

impl IssueGrantOperation {
    pub fn new(config: IssueGrantConfig) -> Self {
        Self { config }
    }

    fn stored(&self, record: GrantRecord) -> Result<(bool, GrantRecord), GrantError> {
        if record.revoked {
            return Err(GrantError::Revoked);
        }
        let config = &self.config;
        check_issued(
            &record.grant,
            &config.realm_id,
            config.job_id.as_ulid(),
            config.now,
        )?;
        Ok((true, record))
    }

    fn sign(&self, checkpoint: ExportCheckpoint) -> Result<(bool, GrantRecord), GrantError> {
        let config = &self.config;
        let selection = &config.selection;
        let facts = checkpoint
            .export_facts(&selection.files)
            .ok_or(GrantError::Unfinished)?;
        let grant = ExportGrant {
            source: config.realm_id,
            audience: selection.audience,
            intent_digest: selection.intent_digest.clone(),
            export_job_id: config.job_id.as_ulid(),
            document_id: config.document_id,
            source_revision: facts.revision,
            dataset_digest: hex::encode(facts.dataset_digest),
            selection_digest: selection_digest(facts.revision, &facts.versions)?,
            artifact_url: config.artifact_url.clone(),
            artifact_blake3: hex::encode(facts.artifact.blake3),
            artifact_size: facts.artifact.size,
            issued_at: config.now,
            expires_at: config.now.saturating_add(MAX_TRANSFER_SECS),
        };
        let record = GrantRecord {
            grant: Signed::sign(grant, &config.capabilities)?,
            principal: config.principal,
            document_path: config.document_path.clone(),
            with_files: !selection.files.is_empty(),
            sources: facts.sources,
            revoked: false,
        };
        Ok((false, record))
    }

    fn decide(&self, result: IoResult) -> Result<(bool, GrantRecord), GrantError> {
        let [record, checkpoint] = values::<2>(result)?;
        if let Some(record) = record {
            return self.stored(decode(&record)?);
        }
        let checkpoint = checkpoint.ok_or(GrantError::Unfinished)?;
        self.sign(decode(&checkpoint)?)
    }
}

impl Operation for IssueGrantOperation {
    /// Whether the record was stored before, and the record.
    type Output = (bool, GrantRecord);
    type Error = GrantError;

    fn first(&mut self) -> Result<Next<Self::Output>, GrantError> {
        let job_id = self.config.job_id;
        Ok(Next::Call(IoCall::Read(vec![
            (FEDERATION_KEYSPACE.to_string(), record_key(job_id).into()),
            (
                JOB_STATE_KEYSPACE.to_string(),
                job_id.to_bytes().to_vec().into(),
            ),
        ])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, GrantError> {
        self.decide(result).map(Next::Done)
    }
}

fn storage_error(error: impl std::fmt::Display) -> GrantError {
    GrantError::Storage(error.to_string())
}

/// The context a stored grant is checked under: its consenting user, without a session.
pub fn grant_principal(user_id: UserId, realm_id: RealmId) -> AuthContext {
    AuthContext {
        user_id,
        realm_id,
        path_restrictions: None,
        session: None,
    }
}

/// READ on the document and the deny-only `federation.export` policies for `audience`.
pub fn export_access(
    auth: &AuthContext,
    document_path: &str,
    audience: RealmId,
    with_files: bool,
) -> AccessCheckOperation {
    let extras = PolicyRequestExtras {
        operation: EXPORT_OPERATION.to_string(),
        params: BTreeMap::from([
            ("destination_realm".to_string(), audience.to_string()),
            ("with_files".to_string(), with_files.to_string()),
        ]),
        ..Default::default()
    };
    let target = (document_path.to_string(), Permission::READ);
    AccessCheckOperation::new(auth.realm_id, auth.clone(), target, extras)
}

#[derive(Debug, PartialEq)]
enum RecheckStep {
    Cutoff,
    Document,
    SourceAccess(usize),
    Pinned(usize),
    ActiveKey(usize),
    Holder(usize, BucketKeyRef),
    Unlocked(usize, BucketKeyRef),
}

type RecheckTurn = Result<Next<()>, GrantError>;

/// Current READ on the document and every pinned version, the export policies, the pinned
/// versions themselves, and for a copy read under a bucket key that key unlocked and held. A
/// credential cutoff of the consenting user also ends grants issued before it.
#[derive(Debug, PartialEq)]
pub struct RecheckGrantOperation {
    auth: AuthContext,
    record: GrantRecord,
    now_secs: u64,
    step: RecheckStep,
}

impl RecheckGrantOperation {
    pub fn new(auth: AuthContext, record: GrantRecord) -> Self {
        Self {
            auth,
            record,
            now_secs: 0,
            step: RecheckStep::Cutoff,
        }
    }

    fn check_cutoff(&mut self, result: IoResult) -> RecheckTurn {
        let [config] = values::<1>(result)?;
        let config = (config.map(|bytes| RealmConfigDocument::from_bytes(&bytes)))
            .transpose()
            .map_err(storage_error)?;
        let cutoff =
            config.and_then(|config| config.user_cutoff(&self.record.principal, self.now_secs));
        if cutoff.is_some_and(|cutoff| self.record.grant.payload.issued_at < cutoff) {
            return Err(GrantError::Denied);
        }
        let record = &self.record;
        let access = export_access(
            &self.auth,
            &record.document_path,
            record.grant.payload.audience,
            record.with_files,
        );
        self.step = RecheckStep::Document;
        Ok(Next::Call(IoCall::child(access)))
    }

    fn next_source(&mut self, index: usize) -> RecheckTurn {
        let Some(source) = self.record.sources.get(index) else {
            return Ok(Next::Done(()));
        };
        let audience = self.record.grant.payload.audience;
        let access = export_access(&self.auth, &source.path, audience, true);
        self.step = RecheckStep::SourceAccess(index);
        Ok(Next::Call(IoCall::child(access)))
    }

    fn source(&self, index: usize) -> Result<&PinnedSource, GrantError> {
        (self.record.sources.get(index)).ok_or_else(|| IoError::Unfinished.into())
    }

    fn read_pinned(&mut self, index: usize) -> RecheckTurn {
        let source = self.source(index)?;
        let key = VersionKey::new(source.bucket.clone(), source.key.clone(), source.version_id);
        let key = key.to_bytes().map_err(storage_error)?;
        self.step = RecheckStep::Pinned(index);
        Ok(Next::Call(read_row(BLOB_VERSIONS_KEYSPACE, key)))
    }

    /// The pinned version must still exist with its hash.
    fn check_pinned(&mut self, index: usize, result: IoResult) -> RecheckTurn {
        let [row] = values::<1>(result)?;
        let row = row.ok_or(GrantError::Denied)?;
        let version = BlobVersion::from_bytes(&row).map_err(storage_error)?;
        let source = self.source(index)?;
        if version.blob_hash() != Some(&source.blake3) {
            return Err(GrantError::Denied);
        }
        match source.key_ref {
            Some(key) => self.check_holder(index, key),
            None => {
                let row = source.bucket.as_bytes().to_vec();
                self.step = RecheckStep::ActiveKey(index);
                Ok(Next::Call(read_row(BUCKET_ENCRYPTION_KEYSPACE, row)))
            }
        }
    }

    /// A plain copy in an encrypting bucket is read under the bucket's active key.
    fn check_active(&mut self, index: usize, result: IoResult) -> RecheckTurn {
        let [row] = values::<1>(result)?;
        let settings = BucketEncryption::from_row(row.as_deref()).map_err(storage_error)?;
        match settings.active_key() {
            Some(key) => self.check_holder(index, key),
            None => self.next_source(index + 1),
        }
    }

    fn check_holder(&mut self, index: usize, key: BucketKeyRef) -> RecheckTurn {
        let bucket = &self.source(index)?.bucket;
        let holder = HolderCheckOperation::new(self.auth.realm_id, bucket, self.auth.user_id);
        self.step = RecheckStep::Holder(index, key);
        Ok(Next::Call(IoCall::child(holder)))
    }

    fn read_status(&mut self, index: usize, key: BucketKeyRef) -> RecheckTurn {
        self.step = RecheckStep::Unlocked(index, key);
        let status = BlobEffect::ReadKeyStatus {
            bucket_id: key.bucket_id,
        };
        Ok(Next::Call(IoCall::from(Effect::Blob(status))))
    }

    /// A node without the blob plane holds no unlocked key.
    fn check_unlocked(&mut self, index: usize, key: BucketKeyRef, result: IoResult) -> RecheckTurn {
        match result.event()? {
            Event::Blob(BlobEvent::KeyStatus { generations })
                if (generations.iter()).any(|status| status.key == key && status.active) =>
            {
                self.next_source(index + 1)
            }
            Event::Blob(BlobEvent::KeyStatus { .. })
            | Event::Blob(BlobEvent::Error(BlobError::HandleMissing)) => Err(GrantError::Denied),
            other => Err(GrantError::Storage(format!(
                "key status failed: {}",
                other.label()
            ))),
        }
    }
}

impl Operation for RecheckGrantOperation {
    type Output = ();
    type Error = GrantError;

    fn first(&mut self) -> RecheckTurn {
        let target = DocumentTarget::RealmConfig {
            realm_id: self.auth.realm_id,
        };
        let read = read_row(target.storage_keyspace(), target.storage_key());
        Ok(Next::Call(read))
    }

    fn next(&mut self, result: IoResult) -> RecheckTurn {
        match self.step {
            RecheckStep::Cutoff => self.check_cutoff(result),
            RecheckStep::Document => match access_granted(result) {
                true => self.next_source(0),
                false => Err(GrantError::Denied),
            },
            RecheckStep::SourceAccess(index) => match access_granted(result) {
                true => self.read_pinned(index),
                false => Err(GrantError::Denied),
            },
            RecheckStep::Pinned(index) => self.check_pinned(index, result),
            RecheckStep::ActiveKey(index) => self.check_active(index, result),
            RecheckStep::Holder(index, key) => {
                let holder = result.child::<bool, PlaintextError>()?;
                if !holder.map_err(storage_error)? {
                    return Err(GrantError::Denied);
                }
                self.read_status(index, key)
            }
            RecheckStep::Unlocked(index, key) => self.check_unlocked(index, key, result),
        }
    }

    fn stamp(&mut self, now_ms: u64) {
        self.now_secs = now_ms / 1_000;
    }
}

#[derive(Debug, PartialEq)]
enum IssueStep {
    Decide,
    Recheck,
    Store,
}

/// Issues the grant of a finished export after the current checks and stores its record. A
/// repeated request returns the stored grant while it is valid; an expired grant needs a new
/// intent and export. A racing request may store its grant or a revocation first; that one wins.
#[derive(Debug, PartialEq)]
pub struct IssueExportOperation {
    auth: AuthContext,
    config: Option<IssueGrantConfig>,
    job_id: JobId,
    decided: Option<(bool, GrantRecord)>,
    step: IssueStep,
}

impl IssueExportOperation {
    pub fn new(auth: AuthContext, config: IssueGrantConfig) -> Self {
        Self {
            auth,
            job_id: config.job_id,
            config: Some(config),
            decided: None,
            step: IssueStep::Decide,
        }
    }
}

impl Operation for IssueExportOperation {
    type Output = Signed<ExportGrant>;
    type Error = GrantError;

    fn first(&mut self) -> Result<Next<Self::Output>, GrantError> {
        let config = self.config.take().ok_or(IoError::Unfinished)?;
        Ok(Next::Call(IoCall::child(IssueGrantOperation::new(config))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, GrantError> {
        match self.step {
            IssueStep::Decide => {
                let decided = result.child::<(bool, GrantRecord), GrantError>()??;
                let recheck = RecheckGrantOperation::new(self.auth.clone(), decided.1.clone());
                self.decided = Some(decided);
                self.step = IssueStep::Recheck;
                Ok(Next::Call(IoCall::child(recheck)))
            }
            IssueStep::Recheck => {
                result.child::<(), GrantError>()??;
                let (stored, record) = self.decided.take().ok_or(IoError::Unfinished)?;
                if stored {
                    return Ok(Next::Done(record.grant));
                }
                self.step = IssueStep::Store;
                let key = record_key(self.job_id);
                let store = RecordChange::StoreGrant { key, record };
                Ok(Next::Call(change_call(store)))
            }
            IssueStep::Store => {
                let outcome = result.child::<RecordOutcome, RecordError>()?;
                match outcome.map_err(storage_error)? {
                    RecordOutcome::Grant(stored) if stored.revoked => Err(GrantError::Revoked),
                    RecordOutcome::Grant(stored) => Ok(Next::Done(stored.grant)),
                    other => Err(GrantError::Storage(format!("unexpected outcome {other:?}"))),
                }
            }
        }
    }
}

/// Admits one artifact read by a grant: its signature, binding and lifetime, the stored record
/// and revoked flag, and the current checks of the consenting user.
#[derive(Debug, PartialEq)]
pub struct AdmitGrantOperation {
    local: RealmId,
    job_id: JobId,
    grant: Signed<ExportGrant>,
    now: u64,
    record: Option<GrantRecord>,
}

impl AdmitGrantOperation {
    pub fn new(local: RealmId, job_id: JobId, grant: Signed<ExportGrant>, now: u64) -> Self {
        Self {
            local,
            job_id,
            grant,
            now,
            record: None,
        }
    }
}

impl Operation for AdmitGrantOperation {
    type Output = GrantRecord;
    type Error = GrantError;

    fn first(&mut self) -> Result<Next<GrantRecord>, GrantError> {
        check_issued(&self.grant, &self.local, self.job_id.as_ulid(), self.now)?;
        let read = read_row(FEDERATION_KEYSPACE, record_key(self.job_id));
        Ok(Next::Call(read))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<GrantRecord>, GrantError> {
        if let Some(record) = self.record.take() {
            result.child::<(), GrantError>()??;
            return Ok(Next::Done(record));
        }
        let record = (decoded::<GrantRecord>(result)?)
            .filter(|record| record.grant == self.grant)
            .ok_or(GrantError::Missing)?;
        if record.revoked {
            return Err(GrantError::Revoked);
        }
        let auth = grant_principal(record.principal, self.local);
        let recheck = RecheckGrantOperation::new(auth, record.clone());
        self.record = Some(record);
        Ok(Next::Call(IoCall::child(recheck)))
    }
}

/// The grant record of one export job, if one was issued.
#[derive(Debug, PartialEq)]
pub struct ReadGrantOperation {
    job_id: JobId,
}

impl ReadGrantOperation {
    pub fn new(job_id: JobId) -> Self {
        Self { job_id }
    }
}

impl Operation for ReadGrantOperation {
    type Output = Option<GrantRecord>;
    type Error = GrantError;

    fn first(&mut self) -> Result<Next<Self::Output>, GrantError> {
        let read = read_row(FEDERATION_KEYSPACE, record_key(self.job_id));
        Ok(Next::Call(read))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, GrantError> {
        Ok(Next::Done(decoded(result)?))
    }
}

/// Revokes the grant of one export for every later read; false when none was issued.
#[derive(Debug, PartialEq)]
pub struct RevokeGrantOperation {
    job_id: JobId,
}

impl RevokeGrantOperation {
    pub fn new(job_id: JobId) -> Self {
        Self { job_id }
    }
}

impl Operation for RevokeGrantOperation {
    type Output = bool;
    type Error = GrantError;

    fn first(&mut self) -> Result<Next<bool>, GrantError> {
        let key = record_key(self.job_id);
        Ok(Next::Call(change_call(RecordChange::RevokeGrant { key })))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<bool>, GrantError> {
        match (result.child::<RecordOutcome, RecordError>()?).map_err(storage_error)? {
            RecordOutcome::Revoked(existed) => Ok(Next::Done(existed)),
            other => Err(GrantError::Storage(format!("unexpected outcome {other:?}"))),
        }
    }
}

#[cfg(test)]
mod tests {
    use aruna_core::calls::ChildOutcome;
    use aruna_core::structs::identity::auth::NodeCapabilities;
    use ed25519_dalek::SigningKey;

    use super::*;

    const NOW: u64 = 100;

    fn capabilities() -> NodeCapabilities {
        NodeCapabilities::management_node(SigningKey::from_bytes(&[4; 32])).unwrap()
    }

    fn local() -> RealmId {
        RealmId::from_bytes(SigningKey::from_bytes(&[4; 32]).verifying_key().to_bytes())
    }

    fn job() -> JobId {
        JobId::from_bytes([7; 16])
    }

    fn record() -> GrantRecord {
        let grant = ExportGrant {
            source: local(),
            audience: RealmId::from_bytes([9; 32]),
            intent_digest: String::new(),
            export_job_id: job().as_ulid(),
            document_id: Ulid::from_bytes([6; 16]),
            source_revision: Ulid::from_bytes([5; 16]),
            dataset_digest: String::new(),
            selection_digest: String::new(),
            artifact_url: Url::parse("https://a.example.org/artifact").unwrap(),
            artifact_blake3: String::new(),
            artifact_size: 1,
            issued_at: NOW,
            expires_at: NOW + 10,
        };
        GrantRecord {
            grant: Signed::sign(grant, &capabilities()).unwrap(),
            principal: UserId::new(Ulid::from_bytes([1; 16]), local()),
            document_path: "/doc".to_string(),
            with_files: false,
            sources: Vec::new(),
            revoked: false,
        }
    }

    fn sourced() -> GrantRecord {
        GrantRecord {
            sources: vec![PinnedSource {
                path: "/doc/file".to_string(),
                bucket: "lab".to_string(),
                key: "file".to_string(),
                version_id: Ulid::from_bytes([3; 16]),
                blake3: [8; 32],
                size: 1,
                key_ref: None,
            }],
            ..record()
        }
    }

    fn auth() -> AuthContext {
        grant_principal(record().principal, local())
    }

    fn child<T, E>(result: Result<T, E>) -> IoResult
    where
        T: Send + std::fmt::Debug + PartialEq + 'static,
        E: Send + std::fmt::Debug + PartialEq + 'static,
    {
        IoResult::Child(ChildOutcome::new(result))
    }

    fn granted() -> IoResult {
        child::<_, IoError>(Ok(true))
    }

    fn no_config() -> IoResult {
        IoResult::Values(vec![None])
    }

    #[test]
    fn recheck_needs_access() {
        let mut check = RecheckGrantOperation::new(auth(), record());
        check.first().unwrap();
        let access = export_access(&auth(), "/doc", record().grant.payload.audience, false);
        let answer = check.next(no_config());
        assert_eq!(answer, Ok(Next::Call(IoCall::child(access))));
        assert_eq!(check.next(granted()), Ok(Next::Done(())));
        let mut denied = RecheckGrantOperation::new(auth(), record());
        denied.next(no_config()).unwrap();
        let answer = denied.next(child::<_, IoError>(Ok(false)));
        assert_eq!(answer, Err(GrantError::Denied));
    }

    #[test]
    fn cutoff_ends_grant() {
        let mut config = RealmConfigDocument::new(local(), Vec::new(), 3);
        config
            .revoked_tokens
            .push(aruna_core::structs::identity::realm::TokenRevocation {
                token_hash: aruna_core::auth::user_cutoff_hash(&record().principal),
                expires_at: aruna_core::auth::user_cutoff_expiry(NOW + 1),
            });
        let bytes = postcard::to_allocvec(&config).unwrap();
        let mut check = RecheckGrantOperation::new(auth(), record());
        check.stamp(NOW * 1_000);
        let answer = check.next(IoResult::Values(vec![Some(bytes.into())]));
        assert_eq!(answer, Err(GrantError::Denied));
    }

    #[test]
    fn gone_version_denies() {
        let mut check = RecheckGrantOperation::new(auth(), sourced());
        check.next(no_config()).unwrap();
        check.next(granted()).unwrap();
        assert!(matches!(
            check.next(granted()),
            Ok(Next::Call(IoCall::Read(_)))
        ));
        let answer = check.next(IoResult::Values(vec![None]));
        assert_eq!(answer, Err(GrantError::Denied));
    }

    #[test]
    fn locked_key_denies() {
        let key = BucketKeyRef::new(Ulid::from_bytes([2; 16]), 1);
        let mut check = RecheckGrantOperation::new(auth(), sourced());
        check.step = RecheckStep::Holder(0, key);
        let status = BlobEffect::ReadKeyStatus {
            bucket_id: key.bucket_id,
        };
        let read = IoCall::from(Effect::Blob(status));
        let answer = check.next(child::<_, PlaintextError>(Ok(true)));
        assert_eq!(answer, Ok(Next::Call(read)));
        let locked = BlobEvent::KeyStatus {
            generations: Vec::new(),
        };
        let answer = check.next(IoResult::Event(Box::new(Event::Blob(locked))));
        assert_eq!(answer, Err(GrantError::Denied));
        // A node without the blob plane denies the same way.
        check.step = RecheckStep::Unlocked(0, key);
        let missing = BlobEvent::Error(BlobError::HandleMissing);
        let answer = check.next(IoResult::Event(Box::new(Event::Blob(missing))));
        assert_eq!(answer, Err(GrantError::Denied));
    }

    fn issue() -> IssueExportOperation {
        let record = record();
        let config = IssueGrantConfig {
            realm_id: local(),
            principal: record.principal,
            job_id: job(),
            document_id: record.grant.payload.document_id,
            document_path: record.document_path,
            selection: ExportSelection {
                files: Vec::new(),
                audience: record.grant.payload.audience,
                intent_digest: String::new(),
            },
            artifact_url: record.grant.payload.artifact_url,
            capabilities: capabilities(),
            now: NOW,
        };
        IssueExportOperation::new(auth(), config)
    }

    #[test]
    fn stored_grant_returned() {
        let mut operation = issue();
        assert!(matches!(
            operation.first(),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        let recheck = RecheckGrantOperation::new(auth(), record());
        let answer = operation.next(child::<_, GrantError>(Ok((true, record()))));
        assert_eq!(answer, Ok(Next::Call(IoCall::child(recheck))));
        let answer = operation.next(child::<_, GrantError>(Ok(())));
        assert_eq!(answer, Ok(Next::Done(record().grant)));
    }

    #[test]
    fn new_grant_stored() {
        let mut operation = issue();
        operation.first().unwrap();
        operation
            .next(child::<_, GrantError>(Ok((false, record()))))
            .unwrap();
        let store = change_call(RecordChange::StoreGrant {
            key: record_key(job()),
            record: record(),
        });
        let answer = operation.next(child::<_, GrantError>(Ok(())));
        assert_eq!(answer, Ok(Next::Call(store)));
        // A racing revocation wins over the new grant.
        let revoked = GrantRecord {
            revoked: true,
            ..record()
        };
        let stored = RecordOutcome::Grant(revoked);
        let answer = operation.next(child::<_, RecordError>(Ok(stored)));
        assert_eq!(answer, Err(GrantError::Revoked));
    }

    fn stored(record: &GrantRecord) -> IoResult {
        IoResult::Values(vec![Some(postcard::to_allocvec(record).unwrap().into())])
    }

    fn admit() -> AdmitGrantOperation {
        AdmitGrantOperation::new(local(), job(), record().grant, NOW)
    }

    #[test]
    fn admits_matching_record() {
        let mut operation = admit();
        let read = read_row(FEDERATION_KEYSPACE, record_key(job()));
        assert_eq!(operation.first(), Ok(Next::Call(read)));
        let recheck = RecheckGrantOperation::new(auth(), record());
        let answer = operation.next(stored(&record()));
        assert_eq!(answer, Ok(Next::Call(IoCall::child(recheck))));
        let answer = operation.next(child::<_, GrantError>(Ok(())));
        assert_eq!(answer, Ok(Next::Done(record())));
    }

    #[test]
    fn refuses_unusable_record() {
        assert_eq!(admit().next(no_config()), Err(GrantError::Missing));
        let revoked = GrantRecord {
            revoked: true,
            ..record()
        };
        assert_eq!(admit().next(stored(&revoked)), Err(GrantError::Revoked));
    }

    #[test]
    fn revoke_reports_existing() {
        let mut operation = RevokeGrantOperation::new(job());
        let revoke = change_call(RecordChange::RevokeGrant {
            key: record_key(job()),
        });
        assert_eq!(operation.first(), Ok(Next::Call(revoke)));
        let answer = operation.next(child::<_, RecordError>(Ok(RecordOutcome::Revoked(true))));
        assert_eq!(answer, Ok(Next::Done(true)));
    }

    #[test]
    fn reads_grant_record() {
        let mut operation = ReadGrantOperation::new(job());
        let read = read_row(FEDERATION_KEYSPACE, record_key(job()));
        assert_eq!(operation.first(), Ok(Next::Call(read)));
        assert_eq!(
            operation.next(stored(&record())),
            Ok(Next::Done(Some(record())))
        );
    }
}
