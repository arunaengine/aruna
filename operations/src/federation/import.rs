//! Destination side of an export from another realm: what an intent may do, which upload it
//! binds, and whether a running import still stands on its consent.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;
use std::time::Duration;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::Effect;
use aruna_core::events::Event;
use aruna_core::federation::Signed;
use aruna_core::keyspaces::{
    DEDUP_INDEX_KEYSPACE, FEDERATION_KEYSPACE, JOB_KEYSPACE, ROCRATE_UPLOAD_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::outbound::{HttpMethod, HttpRequest, OutboundEffect, OutboundEvent};
use aruna_core::rows::{decode, decoded, read_row, values};
use aruna_core::structs::execution::job::{
    ImportRoCrateSource, ImportRoCrateSpec, JobRecord, RoCrateUploadRecord, dedup_index_key,
    job_record_key, parse_dedup_value, upload_key as stored_upload_key, upload_stale,
    user_dedup_key,
};
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::storage::blob::{BucketInfo, bucket_permission_path};
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::transfer::{
    ExportGrant, ImportDestination, ImportIntent, TransferError, check_grant, check_intent,
};
use aruna_core::{NodeId, UserId};
use serde::Serialize;
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

use crate::auth::request_policy::PolicyRequestExtras;
use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::federation::access::{AccessCheckOperation, access_granted};
use crate::federation::records::{
    ImportRecord, RecordChange, RecordError, RecordOutcome, StaleUpload, change_call,
};
use crate::s3::bucket::get::{GetBucketError, GetBucketOperation};

const IMPORT_OPERATION: &str = "federation.import";

#[derive(Debug, Error, PartialEq)]
pub enum ImportError {
    #[error("the import into this destination is not allowed")]
    Denied,
    #[error("the import does not match its intent")]
    Unbound,
    #[error("the destination bucket does not exist")]
    NoBucket,
    #[error("the import key is bound to another transfer")]
    Conflict,
    #[error("the import waits for a fresh intent and grant")]
    Expired,
    #[error("import storage failed: {0}")]
    Storage(String),
    #[error("the source realm could not be reached: {0}")]
    Unreachable(String),
    #[error("the source realm answered {0}")]
    Refused(reqwest::StatusCode),
}

/// Header carrying a signed export grant as unpadded base64url JSON.
pub const GRANT_HEADER: &str = "x-aruna-export-grant";

/// The unpadded base64url JSON of a signed transfer value, as a header carries it.
pub fn header_value<T: Serialize>(value: &T) -> Result<String, serde_json::Error> {
    use base64::Engine;
    let json = serde_json::to_vec(value)?;
    Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(json))
}

impl From<IoError> for ImportError {
    fn from(error: IoError) -> Self {
        Self::Storage(error.to_string())
    }
}

fn storage_error(error: impl std::fmt::Display) -> ImportError {
    ImportError::Storage(error.to_string())
}

fn record_key(upload_id: Ulid) -> Vec<u8> {
    [&b"import/"[..], &upload_id.to_bytes()].concat()
}

fn binding_key(principal: UserId, import_key: &str) -> Vec<u8> {
    [
        &b"upload/"[..],
        &principal.to_bytes(),
        import_key.as_bytes(),
    ]
    .concat()
}

/// A failed or busy source stays temporary; only other answers refuse the grant.
pub fn answer_error(status: reqwest::StatusCode) -> ImportError {
    use reqwest::StatusCode;
    if status.is_server_error()
        || status == StatusCode::TOO_MANY_REQUESTS
        || status == StatusCode::REQUEST_TIMEOUT
    {
        ImportError::Unreachable(status.to_string())
    } else {
        ImportError::Refused(status)
    }
}

/// The stored record of an imported upload.
#[derive(Debug, PartialEq)]
pub struct ReadImportOperation {
    upload_id: Ulid,
}

impl ReadImportOperation {
    pub fn new(upload_id: Ulid) -> Self {
        Self { upload_id }
    }
}

impl Operation for ReadImportOperation {
    type Output = Option<ImportRecord>;
    type Error = ImportError;

    fn first(&mut self) -> Result<Next<Self::Output>, ImportError> {
        let read = read_row(FEDERATION_KEYSPACE, record_key(self.upload_id));
        Ok(Next::Call(read))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, ImportError> {
        Ok(Next::Done(decoded(result)?))
    }
}

/// The upload `principal`'s import key was bound to by an earlier transfer.
#[derive(Debug, PartialEq)]
pub struct BoundUploadOperation {
    key: Vec<u8>,
}

impl BoundUploadOperation {
    pub fn new(principal: UserId, import_key: &str) -> Self {
        Self {
            key: binding_key(principal, import_key),
        }
    }
}

fn bound_answer(result: IoResult) -> Result<Option<Ulid>, ImportError> {
    let [row] = values::<1>(result)?;
    row.map(|row| {
        let bytes = <[u8; 16]>::try_from(row.as_ref())
            .map_err(|_| ImportError::Storage("invalid upload binding".to_string()))?;
        Ok(Ulid::from_bytes(bytes))
    })
    .transpose()
}

impl Operation for BoundUploadOperation {
    type Output = Option<Ulid>;
    type Error = ImportError;

    fn first(&mut self) -> Result<Next<Self::Output>, ImportError> {
        Ok(Next::Call(read_row(FEDERATION_KEYSPACE, self.key.clone())))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, ImportError> {
        bound_answer(result).map(Next::Done)
    }
}

#[derive(Debug, PartialEq)]
enum ConfirmStep {
    Record,
    Consume,
    Source,
}

/// Before a bound import starts or resumes: a confirmation the source gave since the last start
/// (a pull or a push), or else a fresh confirmation of the grant by the source now.
#[derive(Debug, PartialEq)]
pub struct ConfirmSourceOperation {
    upload_id: Option<Ulid>,
    timeout: Duration,
    grant: Option<Signed<ExportGrant>>,
    step: ConfirmStep,
}

impl ConfirmSourceOperation {
    pub fn new(spec: &ImportRoCrateSpec, timeout: Duration) -> Self {
        let upload_id = match &spec.source {
            ImportRoCrateSource::Upload { upload_id } => Some(*upload_id),
            _ => None,
        };
        Self {
            upload_id,
            timeout,
            grant: None,
            step: ConfirmStep::Record,
        }
    }

    fn head_call(&self, grant: &Signed<ExportGrant>) -> Result<IoCall, ImportError> {
        let header = header_value(grant).map_err(storage_error)?;
        let request = HttpRequest {
            method: HttpMethod::Head,
            url: grant.payload.artifact_url.to_string(),
            headers: vec![(GRANT_HEADER, header)],
            body: Default::default(),
            deadline: self.timeout,
        };
        let effect = Effect::Outbound(OutboundEffect::Request(Box::new(request)));
        Ok(IoCall::from(effect))
    }
}

impl Operation for ConfirmSourceOperation {
    type Output = ();
    type Error = ImportError;

    fn first(&mut self) -> Result<Next<()>, ImportError> {
        let Some(upload_id) = self.upload_id else {
            return Ok(Next::Done(()));
        };
        let read = read_row(FEDERATION_KEYSPACE, record_key(upload_id));
        Ok(Next::Call(read))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, ImportError> {
        match self.step {
            ConfirmStep::Record => {
                let upload_id = self.upload_id.ok_or(IoError::Unfinished)?;
                let Some(record) = decoded::<ImportRecord>(result)? else {
                    return Ok(Next::Done(()));
                };
                self.grant = Some(record.grant);
                self.step = ConfirmStep::Consume;
                let change = RecordChange::ConsumeConfirmation {
                    record_key: record_key(upload_id),
                };
                Ok(Next::Call(change_call(change)))
            }
            ConfirmStep::Consume => {
                let outcome = result.child::<RecordOutcome, RecordError>()?;
                if outcome.map_err(storage_error)? == RecordOutcome::Confirmed(true) {
                    return Ok(Next::Done(()));
                }
                let grant = self.grant.take().ok_or(IoError::Unfinished)?;
                self.step = ConfirmStep::Source;
                Ok(Next::Call(self.head_call(&grant)?))
            }
            ConfirmStep::Source => {
                let Event::Outbound(OutboundEvent::Answered { result }) = result.event()? else {
                    let expected = "OutboundEvent::Answered";
                    let got = "another outbound event";
                    return Err(IoError::Unexpected { expected, got }.into());
                };
                let status = result.map_err(|error| ImportError::Unreachable(error.to_string()))?;
                let status = reqwest::StatusCode::from_u16(status)
                    .map_err(|error| ImportError::Unreachable(error.to_string()))?;
                if !status.is_success() {
                    return Err(answer_error(status));
                }
                Ok(Next::Done(()))
            }
        }
    }
}

/// WRITE on the destination bucket and metadata path, and the deny-only `federation.import`
/// policies, which see `source_realm` once the source is known.
#[derive(Debug, PartialEq)]
pub struct AuthorizeImportOperation {
    auth: AuthContext,
    destination: ImportDestination,
    source: Option<RealmId>,
    node_id: NodeId,
    /// Paths still to check, the next one last; `None` until the bucket is read.
    paths: Option<Vec<String>>,
}

impl AuthorizeImportOperation {
    pub fn new(
        auth: AuthContext,
        destination: ImportDestination,
        source: Option<RealmId>,
        node_id: NodeId,
    ) -> Self {
        Self {
            auth,
            destination,
            source,
            node_id,
            paths: None,
        }
    }

    fn set_paths(&mut self, bucket: &BucketInfo) {
        let realm_id = self.auth.realm_id;
        let destination = &self.destination;
        let metadata = MetadataRegistryRecord::normalize_document_path(&destination.metadata_path);
        let bucket_path =
            bucket_permission_path(realm_id, bucket.group_id, self.node_id, &destination.bucket);
        let metadata_path = format!("/{realm_id}/g/{}/meta/{metadata}", destination.group_id);
        self.paths = Some(vec![metadata_path, bucket_path]);
    }

    fn next_path(&mut self) -> Result<Next<()>, ImportError> {
        let Some(path) = self.paths.as_mut().and_then(Vec::pop) else {
            return Ok(Next::Done(()));
        };
        let extras = PolicyRequestExtras {
            operation: IMPORT_OPERATION.to_string(),
            params: (self.source)
                .map(|source| BTreeMap::from([("source_realm".to_string(), source.to_string())]))
                .unwrap_or_default(),
            ..Default::default()
        };
        let target = (path, Permission::WRITE);
        let access =
            AccessCheckOperation::new(self.auth.realm_id, self.auth.clone(), target, extras);
        Ok(Next::Call(IoCall::child(access)))
    }
}

impl Operation for AuthorizeImportOperation {
    type Output = ();
    type Error = ImportError;

    fn first(&mut self) -> Result<Next<()>, ImportError> {
        let bucket = GetBucketOperation::new(self.destination.bucket.clone());
        Ok(Next::Call(IoCall::child(bucket)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, ImportError> {
        if self.paths.is_some() {
            if !access_granted(result) {
                return Err(ImportError::Denied);
            }
            return self.next_path();
        }
        match result.child::<BucketInfo, GetBucketError>()? {
            Ok(bucket) => self.set_paths(&bucket),
            Err(GetBucketError::NotFound) => return Err(ImportError::NoBucket),
            Err(error) => return Err(storage_error(error)),
        }
        self.next_path()
    }
}

#[derive(Debug, PartialEq)]
enum StaleStep {
    Upload,
    Dedup,
    Job,
}

/// Whether a bound upload can no longer start an import under [`upload_stale`], with the import
/// jobs of its principal. The binding transaction checks it again before replacing it.
#[derive(Debug, PartialEq)]
pub struct StaleUploadOperation {
    principal: UserId,
    import_key: String,
    upload_id: Ulid,
    now_ms: u64,
    step: StaleStep,
}

impl StaleUploadOperation {
    fn new(principal: UserId, import_key: &str, upload_id: Ulid, now_ms: u64) -> Self {
        Self {
            principal,
            import_key: import_key.to_string(),
            upload_id,
            now_ms,
            step: StaleStep::Upload,
        }
    }
}

impl Operation for StaleUploadOperation {
    type Output = bool;
    type Error = ImportError;

    fn first(&mut self) -> Result<Next<bool>, ImportError> {
        let key = stored_upload_key(self.upload_id);
        Ok(Next::Call(read_row(ROCRATE_UPLOAD_KEYSPACE, key)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<bool>, ImportError> {
        let [row] = values::<1>(result)?;
        match self.step {
            StaleStep::Upload => {
                if let Some(row) = row {
                    let upload: RoCrateUploadRecord = decode(&row)?;
                    return Ok(Next::Done(upload_stale(Some(&upload), false, self.now_ms)));
                }
                let dedup_key = user_dedup_key(self.principal, &self.import_key);
                self.step = StaleStep::Dedup;
                let key = dedup_index_key(self.principal, &dedup_key);
                Ok(Next::Call(read_row(DEDUP_INDEX_KEYSPACE, key)))
            }
            StaleStep::Dedup => {
                let Some(row) = row else {
                    return Ok(Next::Done(upload_stale(None, false, self.now_ms)));
                };
                let (job_id, _) = parse_dedup_value(row.as_ref()).map_err(storage_error)?;
                self.step = StaleStep::Job;
                Ok(Next::Call(read_row(JOB_KEYSPACE, job_record_key(job_id))))
            }
            StaleStep::Job => {
                let held = row.is_some_and(|row| match JobRecord::from_bytes(&row) {
                    Ok(_) => true,
                    Err(error) => {
                        warn!(%error, "Malformed job record retained");
                        false
                    }
                });
                Ok(Next::Done(upload_stale(None, held, self.now_ms)))
            }
        }
    }
}

/// Whether a stored binding and its upload belong to the same transfer as `grant` for `intent`:
/// source, document, digests, artifact, destination and owner.
fn same_transfer(
    stored: &ImportRecord,
    upload: Option<&RoCrateUploadRecord>,
    intent: &ImportIntent,
    grant: &ExportGrant,
) -> bool {
    let old = &stored.grant.payload;
    let artifact = |blake3: &[u8; 32], size: u64| {
        hex::encode(blake3) == grant.artifact_blake3 && size == grant.artifact_size
    };
    old.source == grant.source
        && old.document_id == grant.document_id
        && old.dataset_digest == grant.dataset_digest
        && old.selection_digest == grant.selection_digest
        && old.artifact_blake3 == grant.artifact_blake3
        && old.artifact_size == grant.artifact_size
        && stored.intent.payload.principal == intent.principal
        && stored.intent.payload.destination == intent.destination
        && upload.is_none_or(|upload| {
            upload.owner == intent.principal && artifact(&upload.blake3, upload.size)
        })
}

/// Refuses a bound upload whose record or upload belongs to another transfer.
#[derive(Debug, PartialEq)]
struct CheckBoundOperation {
    upload_id: Ulid,
    intent: ImportIntent,
    grant: ExportGrant,
}

impl Operation for CheckBoundOperation {
    type Output = ();
    type Error = ImportError;

    fn first(&mut self) -> Result<Next<()>, ImportError> {
        Ok(Next::Call(IoCall::Read(vec![
            (
                FEDERATION_KEYSPACE.to_string(),
                record_key(self.upload_id).into(),
            ),
            (
                ROCRATE_UPLOAD_KEYSPACE.to_string(),
                stored_upload_key(self.upload_id),
            ),
        ])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, ImportError> {
        let [stored, upload] = values::<2>(result)?;
        let stored = stored
            .ok_or_else(|| ImportError::Storage("upload binding without record".to_string()))?;
        let stored: ImportRecord = decode(&stored)?;
        let upload: Option<RoCrateUploadRecord> = upload.map(|row| decode(&row)).transpose()?;
        if !same_transfer(&stored, upload.as_ref(), &self.intent, &self.grant) {
            return Err(ImportError::Conflict);
        }
        Ok(Next::Done(()))
    }
}

fn check_bound_call(upload_id: Ulid, intent: &ImportIntent, grant: &ExportGrant) -> IoCall {
    IoCall::child(CheckBoundOperation {
        upload_id,
        intent: intent.clone(),
        grant: grant.clone(),
    })
}

fn bound_outcome(result: IoResult) -> Result<Ulid, ImportError> {
    match result.child::<RecordOutcome, RecordError>()? {
        Ok(RecordOutcome::Bound(bound)) => Ok(bound),
        Ok(other) => Err(ImportError::Storage(format!(
            "unexpected outcome {other:?}"
        ))),
        Err(error) => Err(storage_error(error)),
    }
}

#[derive(Debug, PartialEq)]
enum WriteStep {
    Bind,
    Stale(Ulid),
    Rebind,
    Check(Ulid),
}

/// Binds a verified upload to its intent and grant, and the import key to that upload, so a
/// retried transfer reuses it. Answers the bound upload; an earlier binding wins a race and is
/// reused only for the same transfer.
#[derive(Debug, PartialEq)]
pub struct WriteImportOperation {
    import_key: String,
    upload_id: Ulid,
    record: ImportRecord,
    now_ms: u64,
    step: WriteStep,
}

impl WriteImportOperation {
    pub fn new(import_key: &str, upload_id: Ulid, record: ImportRecord) -> Self {
        Self {
            import_key: import_key.to_string(),
            upload_id,
            record,
            now_ms: 0,
            step: WriteStep::Bind,
        }
    }

    fn principal(&self) -> UserId {
        self.record.intent.payload.principal
    }

    fn bind_call(&self, stale: Option<StaleUpload>) -> IoCall {
        change_call(RecordChange::BindImport {
            key: binding_key(self.principal(), &self.import_key),
            record_key: record_key(self.upload_id),
            upload_id: self.upload_id,
            record: self.record.clone(),
            stale,
        })
    }

    fn check_or_done(&mut self, bound: Ulid) -> Result<Next<Ulid>, ImportError> {
        if bound == self.upload_id {
            return Ok(Next::Done(bound));
        }
        self.step = WriteStep::Check(bound);
        let (intent, grant) = (&self.record.intent.payload, &self.record.grant.payload);
        Ok(Next::Call(check_bound_call(bound, intent, grant)))
    }
}

impl Operation for WriteImportOperation {
    type Output = Ulid;
    type Error = ImportError;

    fn first(&mut self) -> Result<Next<Ulid>, ImportError> {
        Ok(Next::Call(self.bind_call(None)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Ulid>, ImportError> {
        match self.step {
            WriteStep::Bind => {
                let bound = bound_outcome(result)?;
                if bound == self.upload_id {
                    return Ok(Next::Done(bound));
                }
                self.step = WriteStep::Stale(bound);
                let stale = StaleUploadOperation::new(
                    self.principal(),
                    &self.import_key,
                    bound,
                    self.now_ms,
                );
                Ok(Next::Call(IoCall::child(stale)))
            }
            WriteStep::Stale(bound) => {
                if !result.child::<bool, ImportError>()?? {
                    return self.check_or_done(bound);
                }
                let dedup_key = user_dedup_key(self.principal(), &self.import_key);
                let stale = StaleUpload {
                    upload_id: bound,
                    dedup_key: dedup_index_key(self.principal(), &dedup_key).to_vec(),
                    now_ms: self.now_ms,
                };
                self.step = WriteStep::Rebind;
                Ok(Next::Call(self.bind_call(Some(stale))))
            }
            WriteStep::Rebind => {
                let bound = bound_outcome(result)?;
                self.check_or_done(bound)
            }
            WriteStep::Check(bound) => {
                result.child::<(), ImportError>()??;
                Ok(Next::Done(bound))
            }
        }
    }

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }
}

#[derive(Debug, PartialEq)]
enum ReuseStep {
    Binding,
    Stale(Ulid),
    Check(Ulid),
}

/// The upload an earlier transfer of the same principal and import key left on this node; a
/// binding of another transfer is a conflict, and a stale upload is replaced by the new one.
#[derive(Debug, PartialEq)]
pub struct ReusableUploadOperation {
    intent: ImportIntent,
    grant: ExportGrant,
    import_key: String,
    now_ms: u64,
    step: ReuseStep,
}

impl ReusableUploadOperation {
    pub fn new(intent: ImportIntent, grant: ExportGrant, import_key: &str) -> Self {
        Self {
            intent,
            grant,
            import_key: import_key.to_string(),
            now_ms: 0,
            step: ReuseStep::Binding,
        }
    }
}

impl Operation for ReusableUploadOperation {
    type Output = Option<Ulid>;
    type Error = ImportError;

    fn first(&mut self) -> Result<Next<Self::Output>, ImportError> {
        let key = binding_key(self.intent.principal, &self.import_key);
        Ok(Next::Call(read_row(FEDERATION_KEYSPACE, key)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, ImportError> {
        match self.step {
            ReuseStep::Binding => {
                let Some(upload_id) = bound_answer(result)? else {
                    return Ok(Next::Done(None));
                };
                self.step = ReuseStep::Stale(upload_id);
                let principal = self.intent.principal;
                let stale =
                    StaleUploadOperation::new(principal, &self.import_key, upload_id, self.now_ms);
                Ok(Next::Call(IoCall::child(stale)))
            }
            ReuseStep::Stale(upload_id) => {
                if result.child::<bool, ImportError>()?? {
                    return Ok(Next::Done(None));
                }
                self.step = ReuseStep::Check(upload_id);
                let check = check_bound_call(upload_id, &self.intent, &self.grant);
                Ok(Next::Call(check))
            }
            ReuseStep::Check(upload_id) => {
                result.child::<(), ImportError>()??;
                Ok(Next::Done(Some(upload_id)))
            }
        }
    }

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }
}

/// Whether an import job still matches its intent's principal and destination.
fn bound(spec: &ImportRoCrateSpec, intent: &ImportIntent) -> bool {
    let destination = &intent.destination;
    let normalized = MetadataRegistryRecord::normalize_document_path;
    spec.auth_context.user_id == intent.principal
        && spec.target.bucket == destination.bucket
        && spec.target.prefix == destination.prefix
        && spec.metadata.group_id == destination.group_id
        && normalized(&spec.metadata.path) == normalized(&destination.metadata_path)
}

#[derive(Debug, PartialEq)]
enum RecheckStep {
    Record,
    Config,
    Authorize,
}

/// Rechecks an import of another realm's artifact at every step; other imports pass. An expired
/// intent or grant, or a superseded descriptor, pauses the job until a fresh consent rebinds it.
#[derive(Debug, PartialEq)]
pub struct RecheckImportOperation {
    spec: ImportRoCrateSpec,
    node_id: NodeId,
    now: u64,
    record: Option<ImportRecord>,
    step: RecheckStep,
}

impl RecheckImportOperation {
    pub fn new(spec: ImportRoCrateSpec, node_id: NodeId, now: u64) -> Self {
        Self {
            spec,
            node_id,
            now,
            record: None,
            step: RecheckStep::Record,
        }
    }

    fn check_consent(&mut self, config: RealmConfigDocument) -> Result<Next<()>, ImportError> {
        let record = self.record.take().ok_or(IoError::Unfinished)?;
        let (intent, now) = (&record.intent.payload, self.now);
        let local = self.spec.auth_context.realm_id;
        // A credential cutoff of the principal also ends intents issued before it.
        if config
            .user_cutoff(&intent.principal, now)
            .is_some_and(|cutoff| intent.issued_at < cutoff)
        {
            return Err(ImportError::Expired);
        }
        let descriptor = config.federation.ok_or(ImportError::Denied)?.descriptor;
        check_intent(&record.intent, &local, &descriptor, None, now)
            .and_then(|()| check_grant(&record.grant, &record.intent, now))
            .map_err(|error| match error {
                TransferError::BadLifetime | TransferError::StaleDescriptor => ImportError::Expired,
                _ => ImportError::Unbound,
            })?;
        self.step = RecheckStep::Authorize;
        let authorize = AuthorizeImportOperation::new(
            self.spec.auth_context.clone(),
            intent.destination.clone(),
            Some(record.grant.payload.source),
            self.node_id,
        );
        Ok(Next::Call(IoCall::child(authorize)))
    }
}

impl Operation for RecheckImportOperation {
    type Output = ();
    type Error = ImportError;

    fn first(&mut self) -> Result<Next<()>, ImportError> {
        let ImportRoCrateSource::Upload { upload_id } = &self.spec.source else {
            return Ok(Next::Done(()));
        };
        let read = read_row(FEDERATION_KEYSPACE, record_key(*upload_id));
        Ok(Next::Call(read))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, ImportError> {
        match self.step {
            RecheckStep::Record => {
                let Some(record) = decoded::<ImportRecord>(result)? else {
                    return Ok(Next::Done(()));
                };
                if !bound(&self.spec, &record.intent.payload) {
                    return Err(ImportError::Unbound);
                }
                self.record = Some(record);
                self.step = RecheckStep::Config;
                let local = self.spec.auth_context.realm_id;
                Ok(Next::Call(IoCall::child(GetConfigOperation::new(local))))
            }
            RecheckStep::Config => {
                let config = result.child::<RealmConfigDocument, GetConfigError>()?;
                self.check_consent(config.map_err(storage_error)?)
            }
            RecheckStep::Authorize => {
                result.child::<(), ImportError>()??;
                Ok(Next::Done(()))
            }
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::driver::{DriverContext, drive};
    use aruna_core::effects::StorageEffect;
    use aruna_core::federation::RealmDescriptor;
    use aruna_core::keyspaces::S3_BUCKET_KEYSPACE;
    use aruna_core::structs::execution::job::{
        ImportMetadataTarget, ImportRoCrateTarget, RoCrateLimits,
    };
    use aruna_core::structs::identity::auth::NodeCapabilities;
    use aruna_core::structs::storage::blob::BucketInfo;
    use aruna_core::transfer::MAX_TRANSFER_SECS;
    use ed25519_dalek::SigningKey;
    use std::time::SystemTime;
    use tempfile::{TempDir, tempdir};
    use url::Url;

    async fn write_import(
        context: &DriverContext,
        import_key: &str,
        upload_id: Ulid,
        record: &ImportRecord,
    ) -> Result<Ulid, ImportError> {
        let write = WriteImportOperation::new(import_key, upload_id, record.clone());
        drive(write, context).await
    }

    async fn read_import(
        context: &DriverContext,
        upload_id: Ulid,
    ) -> Result<Option<ImportRecord>, ImportError> {
        drive(ReadImportOperation::new(upload_id), context).await
    }

    async fn confirm_source(
        context: &DriverContext,
        spec: &ImportRoCrateSpec,
        timeout: Duration,
    ) -> Result<(), ImportError> {
        drive(ConfirmSourceOperation::new(spec, timeout), context).await
    }

    async fn recheck_import(
        context: &DriverContext,
        spec: &ImportRoCrateSpec,
        node_id: NodeId,
        now: u64,
    ) -> Result<(), ImportError> {
        drive(
            RecheckImportOperation::new(spec.clone(), node_id, now),
            context,
        )
        .await
    }

    async fn authorize_import(
        context: &DriverContext,
        auth: &AuthContext,
        destination: &ImportDestination,
        source: Option<RealmId>,
        node_id: NodeId,
    ) -> Result<(), ImportError> {
        let authorize =
            AuthorizeImportOperation::new(auth.clone(), destination.clone(), source, node_id);
        drive(authorize, context).await
    }

    async fn reusable_upload(
        context: &DriverContext,
        intent: &ImportIntent,
        grant: &ExportGrant,
        import_key: &str,
    ) -> Result<Option<Ulid>, ImportError> {
        let reuse = ReusableUploadOperation::new(intent.clone(), grant.clone(), import_key);
        drive(reuse, context).await
    }

    async fn bound_upload(
        context: &DriverContext,
        principal: UserId,
        import_key: &str,
    ) -> Result<Option<Ulid>, ImportError> {
        drive(BoundUploadOperation::new(principal, import_key), context).await
    }

    fn child<T, E>(result: Result<T, E>) -> IoResult
    where
        T: Send + std::fmt::Debug + PartialEq + 'static,
        E: Send + std::fmt::Debug + PartialEq + 'static,
    {
        IoResult::Child(aruna_core::calls::ChildOutcome::new(result))
    }

    fn row<T: Serialize>(value: &T) -> IoResult {
        IoResult::Values(vec![Some(postcard::to_allocvec(value).unwrap().into())])
    }

    fn absent() -> IoResult {
        IoResult::Values(vec![None])
    }

    fn answered(result: Result<u16, aruna_core::outbound::OutboundError>) -> IoResult {
        let event = Event::Outbound(OutboundEvent::Answered { result });
        IoResult::Event(Box::new(event))
    }

    const TIMEOUT: Duration = Duration::from_secs(1);

    fn confirming() -> ConfirmSourceOperation {
        let upload_id = Ulid::from_bytes([9; 16]);
        let mut operation = ConfirmSourceOperation::new(&spec(upload_id), TIMEOUT);
        let read = read_row(FEDERATION_KEYSPACE, record_key(upload_id));
        assert_eq!(operation.first(), Ok(Next::Call(read)));
        let consume = change_call(RecordChange::ConsumeConfirmation {
            record_key: record_key(upload_id),
        });
        assert_eq!(operation.next(row(&record())), Ok(Next::Call(consume)));
        operation
    }

    fn consumed(confirmed: bool) -> IoResult {
        child::<_, RecordError>(Ok(RecordOutcome::Confirmed(confirmed)))
    }

    #[test]
    fn stored_confirmation_suffices() {
        let mut operation = confirming();
        assert_eq!(operation.next(consumed(true)), Ok(Next::Done(())));
    }

    #[test]
    fn source_confirms_again() {
        let mut operation = confirming();
        let Ok(Next::Call(IoCall::Effect(effect))) = operation.next(consumed(false)) else {
            panic!("expected the source request");
        };
        let Effect::Outbound(OutboundEffect::Request(request)) = *effect else {
            panic!("expected an outbound request");
        };
        assert_eq!(request.method, HttpMethod::Head);
        assert_eq!(request.headers[0].0, GRANT_HEADER);
        assert_eq!(operation.next(answered(Ok(204))), Ok(Next::Done(())));
    }

    #[test]
    fn source_answers_map() {
        let answer = |result| {
            let mut operation = confirming();
            operation.next(consumed(false)).unwrap();
            operation.next(answered(result))
        };
        let refused = reqwest::StatusCode::FORBIDDEN;
        assert_eq!(answer(Ok(403)), Err(ImportError::Refused(refused)));
        assert!(matches!(answer(Ok(503)), Err(ImportError::Unreachable(_))));
        let down = aruna_core::outbound::OutboundError::Retryable("down".to_string());
        assert!(matches!(
            answer(Err(down)),
            Err(ImportError::Unreachable(_))
        ));
    }

    #[test]
    fn plain_import_unconfirmed() {
        let mut plain = spec(Ulid::nil());
        plain.source = ImportRoCrateSource::Object {
            bucket: "lab".to_string(),
            key: "crate.zip".to_string(),
            version: None,
        };
        let mut operation = ConfirmSourceOperation::new(&plain, TIMEOUT);
        assert_eq!(operation.first(), Ok(Next::Done(())));
    }

    fn authorizing() -> AuthorizeImportOperation {
        let auth = spec(Ulid::nil()).auth_context;
        let destination = record().intent.payload.destination;
        AuthorizeImportOperation::new(auth, destination, Some(realm(1)), node())
    }

    fn bucket() -> BucketInfo {
        BucketInfo {
            group_id: Ulid::from_bytes([5; 16]),
            created_at: SystemTime::UNIX_EPOCH,
            created_by: UserId::default(),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: aruna_core::structs::storage::format::Compression::Off,
            distribution: None,
        }
    }

    #[test]
    fn import_needs_both_paths() {
        let mut operation = authorizing();
        assert!(matches!(
            operation.first(),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        let found = child::<_, GetBucketError>(Ok(bucket()));
        assert!(matches!(
            operation.next(found),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        let granted = || child::<_, IoError>(Ok(true));
        assert!(matches!(
            operation.next(granted()),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        assert_eq!(operation.next(granted()), Ok(Next::Done(())));
    }

    #[test]
    fn import_refusals_map() {
        let mut missing = authorizing();
        let answer = missing.next(child::<BucketInfo, _>(Err(GetBucketError::NotFound)));
        assert_eq!(answer, Err(ImportError::NoBucket));
        let mut denied = authorizing();
        denied
            .next(child::<_, GetBucketError>(Ok(bucket())))
            .unwrap();
        let answer = denied.next(child::<_, IoError>(Ok(false)));
        assert_eq!(answer, Err(ImportError::Denied));
    }

    fn stale_check() -> StaleUploadOperation {
        StaleUploadOperation::new(principal(), "key", Ulid::from_bytes([7; 16]), 10)
    }

    #[test]
    fn gone_upload_without_job() {
        let mut operation = stale_check();
        let read = read_row(
            ROCRATE_UPLOAD_KEYSPACE,
            stored_upload_key(Ulid::from_bytes([7; 16])),
        );
        assert_eq!(operation.first(), Ok(Next::Call(read)));
        let dedup = dedup_index_key(principal(), &user_dedup_key(principal(), "key"));
        let read = read_row(DEDUP_INDEX_KEYSPACE, dedup);
        assert_eq!(operation.next(absent()), Ok(Next::Call(read)));
        let expected = upload_stale(None, false, 10);
        assert_eq!(operation.next(absent()), Ok(Next::Done(expected)));
    }

    #[test]
    fn stored_upload_decides() {
        let stored = upload(5);
        let mut operation = stale_check();
        let expected = upload_stale(Some(&stored), false, 10);
        assert_eq!(operation.next(row(&stored)), Ok(Next::Done(expected)));
    }

    fn writing() -> WriteImportOperation {
        let mut operation = WriteImportOperation::new("key", Ulid::from_bytes([1; 16]), record());
        operation.stamp(10);
        operation
    }

    fn bound_to(upload_id: Ulid) -> IoResult {
        child::<_, RecordError>(Ok(RecordOutcome::Bound(upload_id)))
    }

    #[test]
    fn own_binding_done() {
        let mut operation = writing();
        let id = Ulid::from_bytes([1; 16]);
        assert!(matches!(
            operation.first(),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        assert_eq!(operation.next(bound_to(id)), Ok(Next::Done(id)));
    }

    #[test]
    fn stale_winner_rebound() {
        let (own, winner) = (Ulid::from_bytes([1; 16]), Ulid::from_bytes([2; 16]));
        let mut operation = writing();
        let stale = StaleUploadOperation::new(principal(), "key", winner, 10);
        let answer = operation.next(bound_to(winner));
        assert_eq!(answer, Ok(Next::Call(IoCall::child(stale))));
        let dedup_key = user_dedup_key(principal(), "key");
        let rebind = operation.bind_call(Some(StaleUpload {
            upload_id: winner,
            dedup_key: dedup_index_key(principal(), &dedup_key).to_vec(),
            now_ms: 10,
        }));
        let answer = operation.next(child::<_, ImportError>(Ok(true)));
        assert_eq!(answer, Ok(Next::Call(rebind)));
        assert_eq!(operation.next(bound_to(own)), Ok(Next::Done(own)));
    }

    #[test]
    fn live_winner_checked() {
        let winner = Ulid::from_bytes([2; 16]);
        let mut operation = writing();
        operation.next(bound_to(winner)).unwrap();
        let (intent, grant) = (record().intent.payload, record().grant.payload);
        let check = check_bound_call(winner, &intent, &grant);
        let answer = operation.next(child::<_, ImportError>(Ok(false)));
        assert_eq!(answer, Ok(Next::Call(check)));
        let refused = child::<(), _>(Err(ImportError::Conflict));
        assert_eq!(operation.next(refused), Err(ImportError::Conflict));
    }

    #[test]
    fn bound_transfer_compared() {
        let (intent, grant) = (record().intent.payload, record().grant.payload);
        let mut check = CheckBoundOperation {
            upload_id: Ulid::from_bytes([2; 16]),
            intent: intent.clone(),
            grant: grant.clone(),
        };
        assert!(matches!(check.first(), Ok(Next::Call(IoCall::Read(_)))));
        let stored = postcard::to_allocvec(&record()).unwrap();
        let same = IoResult::Values(vec![Some(stored.into()), None]);
        assert_eq!(check.next(same), Ok(Next::Done(())));
        let missing = IoResult::Values(vec![None, None]);
        assert!(matches!(check.next(missing), Err(ImportError::Storage(_))));
    }

    fn reusing() -> ReusableUploadOperation {
        let (intent, grant) = (record().intent.payload, record().grant.payload);
        ReusableUploadOperation::new(intent, grant, "key")
    }

    #[test]
    fn unbound_key_unused() {
        let mut operation = reusing();
        let key = binding_key(principal(), "key");
        let read = read_row(FEDERATION_KEYSPACE, key);
        assert_eq!(operation.first(), Ok(Next::Call(read)));
        assert_eq!(operation.next(absent()), Ok(Next::Done(None)));
    }

    #[test]
    fn live_upload_reused() {
        let id = Ulid::from_bytes([2; 16]);
        let binding = IoResult::Values(vec![Some(id.to_bytes().to_vec().into())]);
        let mut operation = reusing();
        assert!(matches!(
            operation.next(binding),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        operation.next(child::<_, ImportError>(Ok(false))).unwrap();
        let answer = operation.next(child::<_, ImportError>(Ok(())));
        assert_eq!(answer, Ok(Next::Done(Some(id))));
    }

    #[test]
    fn binding_row_checked() {
        let mut read = BoundUploadOperation::new(principal(), "key");
        read.first().unwrap();
        let garbled = IoResult::Values(vec![Some(vec![1, 2].into())]);
        assert!(matches!(read.next(garbled), Err(ImportError::Storage(_))));
        let mut imported = ReadImportOperation::new(Ulid::from_bytes([2; 16]));
        imported.first().unwrap();
        assert_eq!(
            imported.next(row(&record())),
            Ok(Next::Done(Some(record())))
        );
    }

    #[test]
    fn foreign_import_rechecked() {
        let upload_id = Ulid::from_bytes([9; 16]);
        let mut recheck = RecheckImportOperation::new(spec(upload_id), node(), 100);
        let read = read_row(FEDERATION_KEYSPACE, record_key(upload_id));
        assert_eq!(recheck.first(), Ok(Next::Call(read)));
        let config = IoCall::child(GetConfigOperation::new(realm(2)));
        assert_eq!(recheck.next(row(&record())), Ok(Next::Call(config)));
        let failed = child::<RealmConfigDocument, _>(Err(GetConfigError::DocumentNotFound));
        assert!(matches!(recheck.next(failed), Err(ImportError::Storage(_))));
        let mut unrecorded = RecheckImportOperation::new(spec(upload_id), node(), 100);
        assert_eq!(unrecorded.next(absent()), Ok(Next::Done(())));
    }

    pub(crate) fn context() -> (TempDir, DriverContext) {
        let dir = tempdir().unwrap();
        let context = DriverContext {
            storage_handle: aruna_storage::FjallStorage::open(dir.path().to_str().unwrap())
                .unwrap(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        };
        (dir, context)
    }

    pub(crate) fn realm(seed: u8) -> RealmId {
        RealmId::from_bytes(
            SigningKey::from_bytes(&[seed; 32])
                .verifying_key()
                .to_bytes(),
        )
    }

    pub(crate) fn signer(seed: u8) -> NodeCapabilities {
        NodeCapabilities::management_node(SigningKey::from_bytes(&[seed; 32])).unwrap()
    }

    pub(crate) fn principal() -> UserId {
        UserId::new(Ulid::from_bytes([3; 16]), realm(1))
    }

    pub(crate) fn descriptor() -> Signed<RealmDescriptor> {
        let descriptor = RealmDescriptor {
            realm_id: realm(2),
            name: "B".to_string(),
            description: String::new(),
            api_url: Url::parse("https://b.example.org/api/v1").unwrap(),
            portal_url: Url::parse("https://b.example.org/").unwrap(),
            contact: None,
            login_providers: Vec::new(),
            issued_at: 1,
        };
        Signed::sign(descriptor, &signer(2)).unwrap()
    }

    /// Stores realm 2's federation settings with `descriptor`.
    async fn seed_config(context: &DriverContext, descriptor: Signed<RealmDescriptor>) {
        seed_cutoff(context, descriptor, None).await;
    }

    pub(crate) async fn seed_cutoff(
        context: &DriverContext,
        descriptor: Signed<RealmDescriptor>,
        cutoff: Option<aruna_core::structs::identity::realm::TokenRevocation>,
    ) {
        use aruna_core::document::DocumentTarget;
        use aruna_core::federation::{AcceptedRealms, FederationSettings, RegistrationMode};
        use aruna_core::structs::identity::auth::Actor;
        use aruna_core::structs::identity::realm::RealmConfigDocument;
        let mut config = RealmConfigDocument::default_for_realm(realm(2), Vec::new());
        config.revoked_tokens.extend(cutoff);
        config.federation = Some(FederationSettings {
            name: "B".to_string(),
            description: String::new(),
            contact: None,
            login_providers: Vec::new(),
            api_url: descriptor.payload.api_url.clone(),
            portal_url: descriptor.payload.portal_url.clone(),
            registry_url: None,
            registration: RegistrationMode::Enabled,
            accepted_realms: AcceptedRealms::None,
            descriptor,
        });
        let actor = Actor {
            node_id: node(),
            user_id: UserId::new(Ulid::from_bytes([2; 16]), realm(2)),
            realm_id: realm(2),
        };
        let target = DocumentTarget::RealmConfig { realm_id: realm(2) };
        let effect = StorageEffect::Write {
            key_space: target.storage_keyspace().to_string(),
            key: target.storage_key().to_vec().into(),
            value: config.to_bytes(&actor).unwrap().into(),
            txn_id: None,
        };
        context.storage_handle.send_storage_effect(effect).await;
    }

    pub(crate) fn record() -> ImportRecord {
        let intent = ImportIntent {
            realm_id: realm(2),
            descriptor_digest: aruna_core::handoff::descriptor_digest(&descriptor()).unwrap(),
            principal: principal(),
            destination: ImportDestination {
                group_id: Ulid::from_bytes([1; 16]),
                bucket: "lab".to_string(),
                prefix: "imports".to_string(),
                metadata_path: "datasets/run".to_string(),
            },
            max_bytes: 100,
            nonce: String::new(),
            issued_at: 1,
            expires_at: 1 + MAX_TRANSFER_SECS,
            intent_id: Ulid::from_bytes([4; 16]),
        };
        let intent = Signed::sign(intent, &signer(2)).unwrap();
        let grant = ExportGrant {
            source: realm(1),
            audience: realm(2),
            intent_digest: aruna_core::transfer::intent_digest(&intent).unwrap(),
            export_job_id: Ulid::from_bytes([5; 16]),
            document_id: Ulid::from_bytes([6; 16]),
            source_revision: Ulid::from_bytes([7; 16]),
            dataset_digest: String::new(),
            selection_digest: String::new(),
            artifact_url: Url::parse("https://a.example.org/artifact").unwrap(),
            artifact_blake3: String::new(),
            artifact_size: 10,
            issued_at: 1,
            expires_at: 1 + MAX_TRANSFER_SECS,
        };
        ImportRecord {
            intent,
            grant: Signed::sign(grant, &signer(1)).unwrap(),
            confirmed: false,
        }
    }

    fn spec(upload_id: Ulid) -> ImportRoCrateSpec {
        ImportRoCrateSpec {
            auth_context: AuthContext {
                user_id: principal(),
                realm_id: realm(2),
                path_restrictions: None,
                session: None,
            },
            source: ImportRoCrateSource::Upload { upload_id },
            target: ImportRoCrateTarget {
                bucket: "lab".to_string(),
                prefix: "imports".to_string(),
            },
            metadata: ImportMetadataTarget {
                group_id: Ulid::from_bytes([1; 16]),
                path: "datasets/run".to_string(),
                public: false,
            },
            limits: RoCrateLimits::default(),
            document_id: Ulid::nil(),
        }
    }

    const NOW: u64 = 100;

    fn node() -> NodeId {
        iroh::SecretKey::from_bytes(&[8; 32]).public()
    }

    #[tokio::test]
    async fn destination_binding_kept() {
        let (_dir, context) = context();
        let upload_id = Ulid::from_bytes([9; 16]);
        // An ordinary upload import has no record and is not rechecked here.
        assert_eq!(
            recheck_import(&context, &spec(upload_id), node(), NOW).await,
            Ok(())
        );
        write_import(&context, "key", upload_id, &record())
            .await
            .unwrap();
        let bound = bound_upload(&context, principal(), "key").await;
        assert_eq!(bound, Ok(Some(upload_id)));
        // Another principal's binding of the same import key is separate.
        let other = UserId::new(Ulid::from_bytes([4; 16]), realm(2));
        assert_eq!(bound_upload(&context, other, "key").await, Ok(None));
        let mut moved = spec(upload_id);
        moved.target.prefix = "elsewhere".to_string();
        let checked = recheck_import(&context, &moved, node(), NOW).await;
        assert_eq!(checked, Err(ImportError::Unbound));
        let mut other = spec(upload_id);
        other.auth_context.user_id = UserId::new(Ulid::from_bytes([4; 16]), realm(2));
        let checked = recheck_import(&context, &other, node(), NOW).await;
        assert_eq!(checked, Err(ImportError::Unbound));
    }

    #[test]
    fn conflicting_reuse_refused() {
        // A binding is reused only for the same transfer, owner and artifact.
        let stored = record();
        let (intent, grant) = (&stored.intent.payload, &stored.grant.payload);
        assert!(same_transfer(&stored, None, intent, grant));
        let mut other = grant.clone();
        other.artifact_blake3 = "ee".repeat(32);
        assert!(!same_transfer(&stored, None, intent, &other));
        let mut moved = intent.clone();
        moved.destination.prefix = "elsewhere".to_string();
        assert!(!same_transfer(&stored, None, &moved, grant));
        let mut upload = upload(0);
        let mut matching = grant.clone();
        matching.artifact_blake3 = hex::encode([0; 32]);
        let mut bound = stored.clone();
        bound.grant.payload = matching.clone();
        assert!(same_transfer(&bound, Some(&upload), intent, &matching));
        upload.owner = UserId::new(Ulid::from_bytes([4; 16]), realm(2));
        assert!(!same_transfer(&bound, Some(&upload), intent, &matching));
    }

    #[tokio::test]
    async fn resumed_import_denied() {
        // A principal without WRITE on the destination is refused when the job resumes.
        let (_dir, context) = context();
        let upload_id = Ulid::from_bytes([9; 16]);
        write_import(&context, "key", upload_id, &record())
            .await
            .unwrap();
        let info = BucketInfo {
            group_id: Ulid::from_bytes([1; 16]),
            created_at: SystemTime::UNIX_EPOCH,
            created_by: principal(),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Default::default(),
            distribution: None,
        };
        let effect = StorageEffect::Write {
            key_space: S3_BUCKET_KEYSPACE.to_string(),
            key: b"lab".to_vec().into(),
            value: info.to_bytes().unwrap().into(),
            txn_id: None,
        };
        context.storage_handle.send_storage_effect(effect).await;
        seed_config(&context, descriptor()).await;
        let checked = recheck_import(&context, &spec(upload_id), node(), NOW).await;
        assert_eq!(checked, Err(ImportError::Denied));
    }

    #[tokio::test]
    async fn expired_consent_pauses() {
        // An expired consent or a superseded descriptor waits for fresh consent.
        let (_dir, context) = context();
        let upload_id = Ulid::from_bytes([9; 16]);
        write_import(&context, "key", upload_id, &record())
            .await
            .unwrap();
        seed_config(&context, descriptor()).await;
        let late = 1 + MAX_TRANSFER_SECS;
        let checked = recheck_import(&context, &spec(upload_id), node(), late).await;
        assert_eq!(checked, Err(ImportError::Expired));
        let mut newer = descriptor().payload;
        newer.issued_at = 2;
        seed_config(&context, Signed::sign(newer, &signer(2)).unwrap()).await;
        let checked = recheck_import(&context, &spec(upload_id), node(), NOW).await;
        assert_eq!(checked, Err(ImportError::Expired));
    }

    #[tokio::test]
    async fn cutoff_ends_intent() {
        // A credential cutoff of the principal after issuance pauses the import.
        let (_dir, context) = context();
        let upload_id = Ulid::from_bytes([9; 16]);
        let record = record();
        write_import(&context, "key", upload_id, &record)
            .await
            .unwrap();
        let cutoff = aruna_core::structs::identity::realm::TokenRevocation {
            token_hash: aruna_core::auth::user_cutoff_hash(&principal()),
            expires_at: aruna_core::auth::user_cutoff_expiry(
                aruna_core::time::unix_timestamp_secs(),
            ),
        };
        seed_cutoff(&context, descriptor(), Some(cutoff)).await;
        let checked = recheck_import(&context, &spec(upload_id), node(), NOW).await;
        assert_eq!(checked, Err(ImportError::Expired));
    }

    #[tokio::test]
    async fn source_confirms_once() {
        // A pull or push confirmation serves one start; a resume asks the source again.
        let (_dir, context) = context();
        let upload_id = Ulid::from_bytes([9; 16]);
        let confirmed = ImportRecord {
            confirmed: true,
            ..record()
        };
        write_import(&context, "key", upload_id, &confirmed)
            .await
            .unwrap();
        let timeout = std::time::Duration::from_secs(1);
        let started = confirm_source(&context, &spec(upload_id), timeout).await;
        assert_eq!(started, Ok(()));
        let stored = read_import(&context, upload_id).await.unwrap().unwrap();
        assert!(!stored.confirmed);
        // The resume asks the source again; this context has no egress, so it cannot.
        let resumed = confirm_source(&context, &spec(upload_id), timeout).await;
        assert!(matches!(resumed, Err(ImportError::Unreachable(_))));
    }

    #[test]
    fn busy_source_waits() {
        // Source infrastructure and throttling defer the import; refusals of the grant end it.
        for code in [500, 502, 503, 504, 429, 408] {
            let status = reqwest::StatusCode::from_u16(code).unwrap();
            assert!(matches!(answer_error(status), ImportError::Unreachable(_)));
        }
        for code in [400, 401, 403, 404, 410] {
            let status = reqwest::StatusCode::from_u16(code).unwrap();
            assert_eq!(answer_error(status), ImportError::Refused(status));
        }
    }

    /// Upload `[9; 16]` of the principal with the size of `record`'s grant.
    fn upload(expires_at_ms: u64) -> RoCrateUploadRecord {
        RoCrateUploadRecord {
            upload_id: Ulid::from_bytes([9; 16]),
            owner: principal(),
            location: aruna_core::structs::storage::blob::BackendLocation {
                backend: aruna_core::structs::storage::blob::BackendRef::node_default(),
                storage_class: None,
                root: "/data".to_string(),
                storage_bucket: "storage".to_string(),
                backend_path: "input".to_string(),
                ulid: Ulid::nil(),
                format: Default::default(),
                created_by: principal(),
                created_at: SystemTime::UNIX_EPOCH,
                staging: false,
                partial: false,
                blob_size: 1,
                hashes: Default::default(),
            },
            blake3: [0; 32],
            size: record().grant.payload.artifact_size,
            media_type: aruna_core::structs::execution::job::RoCrateMediaType::Zip,
            expires_at_ms,
            claimed_by: None,
        }
    }

    #[tokio::test]
    async fn racing_binding_checked() {
        // The live winner of a racing binding serves only the same transfer; another one is
        // refused. Once that upload is gone without an import job, a new upload replaces it.
        let (_dir, context) = context();
        let (first, second) = (Ulid::from_bytes([9; 16]), Ulid::from_bytes([10; 16]));
        let live = upload(u64::MAX);
        let storage = &context.storage_handle;
        crate::jobs::import::upload_store::write_rocrate_upload(&context, &live)
            .await
            .unwrap();
        let mut record = record();
        record.grant.payload.artifact_blake3 = hex::encode(live.blake3);
        write_import(&context, "key", first, &record).await.unwrap();
        let same = write_import(&context, "key", second, &record).await;
        assert_eq!(same, Ok(first));
        let mut other = record.clone();
        other.grant.payload.artifact_size += 1;
        let conflict = write_import(&context, "key", second, &other).await;
        assert_eq!(conflict, Err(ImportError::Conflict));
        let swept = StorageEffect::Delete {
            key_space: aruna_core::keyspaces::ROCRATE_UPLOAD_KEYSPACE.to_string(),
            key: first.to_bytes().to_vec().into(),
            txn_id: None,
        };
        storage.send_storage_effect(swept).await;
        let replaced = write_import(&context, "key", second, &record).await;
        assert_eq!(replaced, Ok(second));
    }
}
