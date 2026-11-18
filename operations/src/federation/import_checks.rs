//! Decides the destination side of a transfer: what an intent may do, which upload it binds,
//! and whether a running import still stands on its consent.
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
use tracing::warn;
use ulid::Ulid;

use crate::auth::request_policy::PolicyRequestExtras;
use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::federation::access::{AccessCheckOperation, access_granted};
use crate::federation::import::{GRANT_HEADER, ImportError, header_value};
use crate::federation::records::{
    ImportRecord, RecordChange, RecordError, RecordOutcome, StaleUpload, change_call,
};
use crate::s3::bucket::get::{GetBucketError, GetBucketOperation};

const IMPORT_OPERATION: &str = "federation.import";

impl From<IoError> for ImportError {
    fn from(error: IoError) -> Self {
        Self::Storage(error.to_string())
    }
}

fn storage_error(error: impl std::fmt::Display) -> ImportError {
    ImportError::Storage(error.to_string())
}

pub(super) fn record_key(upload_id: Ulid) -> Vec<u8> {
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
pub(super) fn answer_error(status: reqwest::StatusCode) -> ImportError {
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
pub(super) struct ReadImportOperation {
    upload_id: Ulid,
}

impl ReadImportOperation {
    pub(super) fn new(upload_id: Ulid) -> Self {
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
pub(super) struct BoundUploadOperation {
    key: Vec<u8>,
}

impl BoundUploadOperation {
    pub(super) fn new(principal: UserId, import_key: &str) -> Self {
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
pub(super) struct ConfirmSourceOperation {
    upload_id: Option<Ulid>,
    timeout: Duration,
    grant: Option<Signed<ExportGrant>>,
    step: ConfirmStep,
}

impl ConfirmSourceOperation {
    pub(super) fn new(spec: &ImportRoCrateSpec, timeout: Duration) -> Self {
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
pub(super) struct AuthorizeImportOperation {
    auth: AuthContext,
    destination: ImportDestination,
    source: Option<RealmId>,
    node_id: NodeId,
    /// Paths still to check, the next one last; `None` until the bucket is read.
    paths: Option<Vec<String>>,
}

impl AuthorizeImportOperation {
    pub(super) fn new(
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
pub(super) struct StaleUploadOperation {
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
pub(super) fn same_transfer(
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
pub(super) struct WriteImportOperation {
    import_key: String,
    upload_id: Ulid,
    record: ImportRecord,
    now_ms: u64,
    step: WriteStep,
}

impl WriteImportOperation {
    pub(super) fn new(import_key: &str, upload_id: Ulid, record: ImportRecord) -> Self {
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
pub(super) struct ReusableUploadOperation {
    intent: ImportIntent,
    grant: ExportGrant,
    import_key: String,
    now_ms: u64,
    step: ReuseStep,
}

impl ReusableUploadOperation {
    pub(super) fn new(intent: ImportIntent, grant: ExportGrant, import_key: &str) -> Self {
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
pub(super) struct RecheckImportOperation {
    spec: ImportRoCrateSpec,
    node_id: NodeId,
    now: u64,
    record: Option<ImportRecord>,
    step: RecheckStep,
}

impl RecheckImportOperation {
    pub(super) fn new(spec: ImportRoCrateSpec, node_id: NodeId, now: u64) -> Self {
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
