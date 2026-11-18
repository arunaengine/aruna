//! Publishes mapped metadata and individual crate files as a native repository record.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, HashSet, VecDeque};

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::operation::Operation;
use aruna_core::repository::invenio::{
    export_fields, missing_fields, record_id, record_identifiers, validate_id,
};
use aruna_core::repository::remote::{FileDigests, InvenioError, RemoteAnswer, RemoteCall};
use aruna_core::repository::{
    ExportIdentity, LinkFailure, RepositoryDestination, RepositoryRecord,
};
use aruna_core::structs::execution::harvest::RepositoryConnectorKind;
use aruna_core::structs::execution::job::{ArtifactRef, ExportRoCrateSpec};
use aruna_core::structs::identity::auth::Permission;
use aruna_core::structs::secondary_id::IdentifierOrigin;
use serde_json::{Value, json};

use super::verify::{
    complete_fields, complete_metadata, matches_access, metadata_digest, verify_file, verify_files,
    verify_metadata,
};
use super::{Session, finished, invalid, json as json_body, json_reply, reply};
use crate::harvest::create_connector::INVENIO_COMMUNITY;
use crate::harvest::read_connector::ConnectorView;
use crate::jobs::export::ExportCheckpoint;
use crate::jobs::import::archive::{ArchiveCompression, ArchiveEntry, ArchiveInspection};
use crate::jobs::repository::check::{unmet, uploads};
use crate::jobs::repository::connector::{
    ConnectOperation, ConnectRequest, ConnectorViewOperation,
};
use crate::jobs::repository::push::{
    GuardOperation, PrepareLinkOperation, Prepared, RecordDraftOperation, checkpoint_saved,
    save_checkpoint,
};
use crate::jobs::repository::{
    Action, JobScope, NodeScope, TransferError, not_supported, remote_call, supports,
};

type Turn<T> = Result<Next<T>, TransferError>;

/// The job and destination of one repository export.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ExportRun {
    pub(crate) scope: NodeScope,
    pub(crate) job: JobScope,
    pub(crate) spec: ExportRoCrateSpec,
    pub(crate) destination: RepositoryDestination,
}

impl ExportRun {
    /// Connects with the personal token; `interruptible` calls end when the job must stop.
    fn connect(&self, missing: &str, interruptible: bool) -> Result<IoCall, TransferError> {
        let credential = self
            .destination
            .credential
            .clone()
            .ok_or_else(|| invalid(missing))?;
        Ok(self.connect_with(Some(credential), interruptible))
    }

    fn connect_with(
        &self,
        credential: Option<aruna_core::repository::RepositoryCredential>,
        interruptible: bool,
    ) -> IoCall {
        IoCall::child(ConnectOperation::new(ConnectRequest {
            scope: self.scope.clone(),
            auth: self.spec.auth_context.clone(),
            group_id: self.destination.group_id,
            connector_id: self.destination.connector_id,
            permission: Permission::WRITE,
            limit: self.spec.limits.metadata_bytes,
            credential,
            stop: interruptible.then(|| self.job.stop.clone()),
        }))
    }

    fn guard(&self) -> IoCall {
        IoCall::child(GuardOperation::new(
            self.job.job_id,
            self.spec.document_id,
            &self.destination,
        ))
    }

    fn file_access(&self) -> &'static str {
        match self.destination.public_files {
            true => "public",
            false => "restricted",
        }
    }
}

fn session_of(result: IoResult) -> Result<Session, TransferError> {
    result.child::<Session, TransferError>()?
}

fn guarded(result: IoResult) -> Result<(), TransferError> {
    result.child::<(), TransferError>()?
}

#[derive(Debug, PartialEq)]
enum After {
    Reserve,
    Metadata,
    Deposit,
}

#[derive(Debug, PartialEq)]
enum ExportStep {
    Link,
    Draft,
    Save(After),
    Record(After),
    Reserve(RepositoryRecord),
    Metadata,
    Deposit(RepositoryRecord, [u8; 32]),
    Observe(TransferError),
    Complete,
}

/// The checkpoint after an export run and how the run ended; the checkpoint keeps what the run
/// stored even when it failed.
pub(crate) type ExportResult = (ExportCheckpoint, Result<(), TransferError>);

/// Runs one export to its repository: draft, reserved DOI, metadata, files and publication, each
/// stored in the checkpoint and on the link. Creating the draft is never cut short by a stop, so
/// a created draft always reaches the checkpoint; later steps stop with the job.
#[derive(Debug, PartialEq)]
pub(crate) struct RepositoryExportOperation {
    kind: RepositoryConnectorKind,
    run: ExportRun,
    checkpoint: ExportCheckpoint,
    inspection: Option<Result<ArchiveInspection, TransferError>>,
    step: ExportStep,
}

impl RepositoryExportOperation {
    pub(crate) fn new(
        kind: RepositoryConnectorKind,
        run: ExportRun,
        checkpoint: ExportCheckpoint,
        inspection: Result<ArchiveInspection, TransferError>,
    ) -> Self {
        Self {
            kind,
            run,
            checkpoint,
            inspection: Some(inspection),
            step: ExportStep::Link,
        }
    }

    fn jsonld(&self) -> Result<String, TransferError> {
        self.checkpoint
            .raw_jsonld
            .clone()
            .ok_or_else(|| invalid("source crate metadata missing"))
    }

    fn record(&self) -> Result<RepositoryRecord, TransferError> {
        self.checkpoint
            .repository
            .clone()
            .ok_or_else(|| invalid("repository draft missing"))
    }

    fn start(&mut self) -> Turn<ExportResult> {
        if self.kind != RepositoryConnectorKind::Invenio {
            return Err(not_supported("deposit"));
        }
        match self.run.destination.link.clone() {
            Some(target) => Ok(Next::Call(IoCall::child(PrepareLinkOperation::new(
                self.run.scope.clone(),
                self.run.job.clone(),
                self.run.spec.clone(),
                self.run.destination.clone(),
                target,
                self.checkpoint.clone(),
            )))),
            None => self.draft(),
        }
    }

    fn draft(&mut self) -> Turn<ExportResult> {
        if self.checkpoint.repository.is_some() {
            return self.reserve();
        }
        let linked = self.run.destination.link.is_some();
        if self.checkpoint.repository_started && self.run.destination.draft_id.is_none() {
            let recovery = match linked {
                true => "inspect the repository drafts and link the dataset again",
                false => "inspect the repository and retry with its draft_id",
            };
            return Err(TransferError::Permanent(format!(
                "draft creation outcome is unknown; {recovery}"
            )));
        }
        let jsonld = self.jsonld()?;
        let mut fenced = self.checkpoint.clone();
        fenced.repository_started = true;
        self.step = ExportStep::Draft;
        Ok(Next::Call(IoCall::child(CreateDraftOperation::new(
            self.run.clone(),
            jsonld,
            self.checkpoint.identity.clone(),
            fenced,
        ))))
    }

    fn reserve(&mut self) -> Turn<ExportResult> {
        let due = self.checkpoint.repository_metadata.is_none()
            && supports(RepositoryConnectorKind::Invenio, Action::ReserveIdentifier);
        let record = self
            .checkpoint
            .repository
            .as_ref()
            .filter(|record| due && record.identifier.is_none());
        let Some(record) = record.cloned() else {
            return self.metadata();
        };
        self.step = ExportStep::Reserve(record.clone());
        Ok(Next::Call(IoCall::child(ReserveDoiOperation::new(
            self.run.clone(),
            record,
        ))))
    }

    fn metadata(&mut self) -> Turn<ExportResult> {
        if self.checkpoint.repository_metadata.is_some() {
            return self.deposit();
        }
        let record = self.record()?;
        let jsonld = self.jsonld()?;
        self.step = ExportStep::Metadata;
        Ok(Next::Call(IoCall::child(PrepareDraftOperation::new(
            self.run.clone(),
            record,
            jsonld,
            self.checkpoint.identity.clone(),
        ))))
    }

    fn deposit(&mut self) -> Turn<ExportResult> {
        let record = self.record()?;
        let artifact = self
            .checkpoint
            .artifact
            .clone()
            .ok_or_else(|| invalid("export artifact missing"))?;
        let metadata = self
            .checkpoint
            .repository_metadata
            .ok_or_else(|| invalid("repository metadata checkpoint missing"))?;
        let uploads = uploads(RepositoryConnectorKind::Invenio, "file", &self.checkpoint)?
            .into_iter()
            .collect::<HashSet<_>>();
        let inspection = self
            .inspection
            .take()
            .ok_or_else(|| invalid("export artifact inspection missing"))?;
        self.step = ExportStep::Deposit(record.clone(), metadata);
        Ok(Next::Call(IoCall::child(DepositOperation::new(
            self.run.clone(),
            record,
            artifact,
            metadata,
            uploads,
            inspection,
        ))))
    }

    fn save_then(&mut self, after: After) -> Turn<ExportResult> {
        self.step = ExportStep::Save(after);
        Ok(Next::Call(save_checkpoint(
            &self.run.job,
            &self.checkpoint,
        )?))
    }

    fn resume(&mut self, after: After) -> Turn<ExportResult> {
        match after {
            After::Reserve => self.reserve(),
            After::Metadata => self.metadata(),
            After::Deposit => self.deposit(),
        }
    }

    fn advance(&mut self, result: IoResult) -> Turn<ExportResult> {
        match std::mem::replace(&mut self.step, ExportStep::Link) {
            ExportStep::Link => {
                let prepared = result.child::<Prepared, TransferError>()??;
                self.run.destination = prepared.destination;
                self.checkpoint.repository_base = prepared.base;
                self.draft()
            }
            ExportStep::Draft => {
                let (fenced, created) = result.child::<CreatedDraft, TransferError>()??;
                self.checkpoint.repository_started |= fenced;
                self.checkpoint.repository = Some(created?);
                self.save_then(After::Reserve)
            }
            ExportStep::Save(after) => {
                checkpoint_saved(result)?;
                let Some(target) = self.run.destination.link.clone() else {
                    return self.resume(after);
                };
                self.step = ExportStep::Record(after);
                Ok(Next::Call(IoCall::child(RecordDraftOperation::new(
                    self.run.scope.clone(),
                    self.run.job.job_id,
                    self.run.spec.document_id,
                    target.link_id,
                    self.record()?,
                ))))
            }
            ExportStep::Record(after) => {
                guarded(result)?;
                self.resume(after)
            }
            ExportStep::Reserve(record) => {
                let reserved = result.child::<RepositoryRecord, TransferError>()??;
                if reserved == record {
                    return self.metadata();
                }
                self.checkpoint.repository = Some(reserved);
                self.save_then(After::Metadata)
            }
            ExportStep::Metadata => {
                let (record, digest) =
                    result.child::<(RepositoryRecord, [u8; 32]), TransferError>()??;
                self.checkpoint.repository = Some(record);
                self.checkpoint.repository_metadata = Some(digest);
                self.save_then(After::Deposit)
            }
            ExportStep::Deposit(record, metadata) => {
                match result.child::<(RepositoryRecord, Vec<String>), TransferError>()? {
                    Ok((record, files)) => {
                        self.checkpoint.repository = Some(record);
                        self.checkpoint.repository_files = files;
                        self.checkpoint.repository_complete = true;
                        self.step = ExportStep::Complete;
                        Ok(Next::Call(save_checkpoint(
                            &self.run.job,
                            &self.checkpoint,
                        )?))
                    }
                    Err(error) if self.run.destination.link.is_some() => {
                        self.step = ExportStep::Observe(error);
                        Ok(Next::Call(IoCall::child(ObserveRevisionOperation::new(
                            self.run.clone(),
                            record,
                            metadata,
                        ))))
                    }
                    Err(error) => Err(error),
                }
            }
            ExportStep::Observe(error) => {
                result.child::<(), TransferError>()??;
                Err(error)
            }
            ExportStep::Complete => {
                checkpoint_saved(result)?;
                Ok(Next::Done((self.checkpoint.clone(), Ok(()))))
            }
        }
    }

    fn settled(&mut self, turn: Turn<ExportResult>) -> Turn<ExportResult> {
        match turn {
            Err(error) => Ok(Next::Done((self.checkpoint.clone(), Err(error)))),
            turn => turn,
        }
    }
}

impl Operation for RepositoryExportOperation {
    type Output = ExportResult;
    type Error = TransferError;

    fn first(&mut self) -> Turn<ExportResult> {
        let turn = self.start();
        self.settled(turn)
    }

    fn next(&mut self, result: IoResult) -> Turn<ExportResult> {
        let turn = self.advance(result);
        self.settled(turn)
    }
}

#[derive(Debug, PartialEq)]
enum ObserveStep {
    Connect,
    Read(Session),
    Record,
}

/// Stores the draft revision after this job's file writes on the link, so the next push does
/// not mistake them for a remote edit. Changed metadata is left for that check. Every failure
/// is only logged.
#[derive(Debug, PartialEq)]
struct ObserveRevisionOperation {
    run: ExportRun,
    record: RepositoryRecord,
    metadata: [u8; 32],
    step: ObserveStep,
}

impl ObserveRevisionOperation {
    fn new(run: ExportRun, record: RepositoryRecord, metadata: [u8; 32]) -> Self {
        Self {
            run,
            record,
            metadata,
            step: ObserveStep::Connect,
        }
    }

    fn advance(&mut self, result: IoResult) -> Turn<()> {
        match std::mem::replace(&mut self.step, ObserveStep::Record) {
            ObserveStep::Connect => {
                let session = session_of(result)?;
                let call = session.get(session.url(&["records", &self.record.id, "draft"])?);
                self.step = ObserveStep::Read(session);
                Ok(Next::Call(call))
            }
            ObserveStep::Read(session) => {
                let current = json_body(result)?;
                let unchanged = metadata_digest(&current) == self.metadata;
                let current = record_from(&session, &current)?;
                let moved = unchanged
                    && !current.published
                    && current.parent_id == self.record.parent_id
                    && current.revision_id != self.record.revision_id;
                let target = self.run.destination.link.as_ref().filter(|_| moved);
                let Some(target) = target else {
                    return Ok(Next::Done(()));
                };
                self.step = ObserveStep::Record;
                Ok(Next::Call(IoCall::child(RecordDraftOperation::new(
                    self.run.scope.clone(),
                    self.run.job.job_id,
                    self.run.spec.document_id,
                    target.link_id,
                    current,
                ))))
            }
            ObserveStep::Record => {
                if let Err(error) = guarded(result) {
                    tracing::warn!(%error, "Recording the draft revision after a failed push failed");
                }
                Ok(Next::Done(()))
            }
        }
    }
}

impl Operation for ObserveRevisionOperation {
    type Output = ();
    type Error = TransferError;

    fn first(&mut self) -> Turn<()> {
        let credential = self.run.destination.credential.clone();
        Ok(Next::Call(self.run.connect_with(credential, false)))
    }

    fn next(&mut self, result: IoResult) -> Turn<()> {
        self.advance(result).or_else(|error| {
            tracing::warn!(%error, "Reading the draft revision after a failed push failed");
            Ok(Next::Done(()))
        })
    }
}

/// Whether the draft creation stored its fence, and the draft it answers.
type CreatedDraft = (bool, Result<RepositoryRecord, TransferError>);

#[derive(Debug, PartialEq)]
enum DraftStep {
    Connect,
    Source(Session, Value),
    Guard(Session, Value, Option<String>),
    Fence(Session, Value, Option<String>),
    Created(Session, Option<String>),
}

/// Finds or creates the export's draft. The checkpoint's fence is stored right before a new
/// draft is requested, so a lost answer is known as an unknown creation on the next run.
#[derive(Debug, PartialEq)]
struct CreateDraftOperation {
    run: ExportRun,
    jsonld: String,
    identity: ExportIdentity,
    fenced_checkpoint: ExportCheckpoint,
    fenced: bool,
    step: DraftStep,
}

impl CreateDraftOperation {
    fn new(
        run: ExportRun,
        jsonld: String,
        identity: ExportIdentity,
        fenced_checkpoint: ExportCheckpoint,
    ) -> Self {
        Self {
            run,
            jsonld,
            identity,
            fenced_checkpoint,
            fenced: false,
            step: DraftStep::Connect,
        }
    }

    fn connected(&mut self, session: Session) -> Turn<CreatedDraft> {
        let destination = &self.run.destination;
        let limit = self.run.spec.limits.metadata_bytes;
        if destination.metadata_json.len() as u64 > limit {
            return Err(invalid("repository metadata exceeds limit"));
        }
        let overrides: Value = serde_json::from_str(&destination.metadata_json)
            .map_err(|_| invalid("invalid repository metadata"))?;
        let document: Value =
            serde_json::from_str(&self.jsonld).map_err(|_| invalid("invalid source crate"))?;
        let fields = mapped_fields(&session, &document, &overrides, &self.identity)?;
        if fields.to_string().len() as u64 > limit {
            return Err(invalid("mapped repository metadata exceeds limit"));
        }
        if let Some(id) = &destination.published_id {
            validate_id(id)?;
            let call = session.get(session.url(&["records", id])?);
            self.step = DraftStep::Source(session, fields);
            return Ok(Next::Call(call));
        }
        self.open_draft(session, fields, None)
    }

    fn open_draft(
        &mut self,
        session: Session,
        mut fields: Value,
        parent: Option<String>,
    ) -> Turn<CreatedDraft> {
        let destination = &self.run.destination;
        if let Some(id) = &destination.draft_id {
            validate_id(id)?;
            let call = session.get(session.url(&["records", id, "draft"])?);
            self.step = DraftStep::Created(session, parent);
            return Ok(Next::Call(call));
        }
        if destination.published_id.is_none() {
            fields["files"] = json!({"enabled": true});
            fields["access"] = json!({"record": "public", "files": self.run.file_access()});
        }
        self.step = DraftStep::Guard(session, fields, parent);
        Ok(Next::Call(self.run.guard()))
    }

    fn check_created(&self, record: &Value, parent: Option<String>) -> Result<(), TransferError> {
        let parent_id = record["parent"]["id"]
            .as_str()
            .ok_or_else(|| invalid("missing draft parent"))?;
        if parent.is_some_and(|parent| parent != parent_id) {
            return Err(invalid("new version belongs to another record"));
        }
        if record["is_published"] != false {
            return Err(invalid("export requires an unpublished draft"));
        }
        let id = record_id(record)?;
        if self
            .run
            .destination
            .draft_id
            .as_ref()
            .is_some_and(|requested| requested != id)
        {
            return Err(invalid("draft identity mismatch"));
        }
        Ok(())
    }

    fn advance(&mut self, result: IoResult) -> Turn<CreatedDraft> {
        match std::mem::replace(&mut self.step, DraftStep::Connect) {
            DraftStep::Connect => {
                let session = session_of(result)?;
                self.connected(session)
            }
            DraftStep::Source(session, fields) => {
                let source = json_body(result)?;
                if source["is_published"] != true {
                    return Err(invalid("new version requires a published record"));
                }
                let parent = source["parent"]["id"]
                    .as_str()
                    .ok_or_else(|| invalid("missing parent identity"))?
                    .to_string();
                self.open_draft(session, fields, Some(parent))
            }
            DraftStep::Guard(session, fields, parent) => {
                guarded(result)?;
                if let Some(id) = &self.run.destination.published_id {
                    let call = session.post(session.url(&["records", id, "versions"])?, None);
                    self.step = DraftStep::Created(session, parent);
                    return Ok(Next::Call(call));
                }
                self.step = DraftStep::Fence(session, fields, parent);
                Ok(Next::Call(save_checkpoint(
                    &self.run.job,
                    &self.fenced_checkpoint,
                )?))
            }
            DraftStep::Fence(session, fields, parent) => {
                checkpoint_saved(result)?;
                self.fenced = true;
                let call = session.post(session.url(&["records"])?, Some(fields));
                self.step = DraftStep::Created(session, parent);
                Ok(Next::Call(call))
            }
            DraftStep::Created(session, parent) => {
                let record = json_body(result)?;
                self.check_created(&record, parent)?;
                Ok(Next::Done((
                    self.fenced,
                    Ok(record_from(&session, &record)?),
                )))
            }
        }
    }
}

impl Operation for CreateDraftOperation {
    type Output = CreatedDraft;
    type Error = TransferError;

    fn first(&mut self) -> Turn<CreatedDraft> {
        let connect = self
            .run
            .connect("a personal repository login is required", false);
        match connect {
            Ok(call) => Ok(Next::Call(call)),
            Err(error) => Ok(Next::Done((false, Err(error)))),
        }
    }

    fn next(&mut self, result: IoResult) -> Turn<CreatedDraft> {
        self.advance(result)
            .or_else(|error| Ok(Next::Done((self.fenced, Err(error)))))
    }
}

/// The record fields mapped from the crate and overrides; a record that would lack a required
/// field is refused with findings before any remote write.
fn mapped_fields(
    session: &Session,
    document: &Value,
    overrides: &Value,
    identity: &ExportIdentity,
) -> Result<Value, TransferError> {
    let fields = export_fields(document, overrides, identity)?;
    let missing = missing_fields(&fields["metadata"], session.endpoint());
    if !missing.is_empty() {
        return Err(unmet(missing));
    }
    Ok(fields)
}

#[derive(Debug, PartialEq)]
enum ReserveStep {
    Connect,
    Guard(Session),
    Reserved(Session),
}

/// Reserves the draft's DOI in its own step, after the draft is stored, so a failed reservation
/// retries without losing the draft.
#[derive(Debug, PartialEq)]
struct ReserveDoiOperation {
    run: ExportRun,
    record: RepositoryRecord,
    step: ReserveStep,
}

impl ReserveDoiOperation {
    fn new(run: ExportRun, record: RepositoryRecord) -> Self {
        Self {
            run,
            record,
            step: ReserveStep::Connect,
        }
    }
}

impl Operation for ReserveDoiOperation {
    type Output = RepositoryRecord;
    type Error = TransferError;

    fn first(&mut self) -> Turn<RepositoryRecord> {
        let call = self
            .run
            .connect("a personal repository login is required", true)?;
        Ok(Next::Call(call))
    }

    fn next(&mut self, result: IoResult) -> Turn<RepositoryRecord> {
        match std::mem::replace(&mut self.step, ReserveStep::Connect) {
            ReserveStep::Connect => {
                self.step = ReserveStep::Guard(session_of(result)?);
                Ok(Next::Call(self.run.guard()))
            }
            ReserveStep::Guard(session) => {
                guarded(result)?;
                let id = &self.record.id;
                let url = session.url(&["records", id, "draft", "pids", "doi"])?;
                let call = session.post(url, None);
                self.step = ReserveStep::Reserved(session);
                Ok(Next::Call(call))
            }
            ReserveStep::Reserved(session) => match json_reply(result)? {
                Ok(reserved)
                    if record_id(&reserved)? == self.record.id
                        && reserved["parent"]["id"] == self.record.parent_id.as_str() =>
                {
                    Ok(Next::Done(record_from(&session, &reserved)?))
                }
                Ok(_) => Err(invalid("draft identity changed while reserving its DOI")),
                Err(error @ (InvenioError::Transport | InvenioError::Status(401 | 403 | 429))) => {
                    Err(error.into())
                }
                // A repository without a DOI provider still publishes; the next push tries again.
                Err(error) => {
                    tracing::warn!(%error, "Reserving the draft DOI failed");
                    Ok(Next::Done(self.record.clone()))
                }
            },
        }
    }
}

/// The repository's record as the link and job see it; drafts point at the draft endpoint.
pub(super) fn record_from(
    session: &Session,
    record: &Value,
) -> Result<RepositoryRecord, TransferError> {
    let id = record_id(record)?.to_string();
    let published = record["is_published"] == true;
    let url = match published {
        true => session.url(&["records", &id])?,
        false => session.url(&["records", &id, "draft"])?,
    };
    Ok(RepositoryRecord {
        url: url.to_string(),
        published,
        parent_id: record["parent"]["id"]
            .as_str()
            .ok_or_else(|| invalid("missing parent identity"))?
            .to_string(),
        revision_id: record["revision_id"]
            .as_u64()
            .ok_or_else(|| invalid("missing record revision"))?,
        identifier: record["pids"]["doi"]["identifier"]
            .as_str()
            .map(str::to_string),
        html_url: page_url(session, record),
        concept_identifier: record["parent"]["pids"]["doi"]["identifier"]
            .as_str()
            .map(str::to_string),
        in_review: false,
        warning: None,
        identifiers: record_identifiers(session.endpoint(), record, IdentifierOrigin::Published),
        id,
    })
}

/// The file keys of a file listing.
pub(super) fn file_list(files: &Value) -> Result<Vec<String>, TransferError> {
    Ok(files["entries"]
        .as_array()
        .ok_or_else(|| invalid("missing repository files"))?
        .iter()
        .filter_map(|file| file["key"].as_str().map(str::to_string))
        .collect())
}

#[derive(Debug, PartialEq)]
enum PrepareStep {
    Connect,
    Current(Session),
    Guard(Session, Value),
    Updated(Value),
}

/// Writes the mapped metadata into the draft and answers the record with its metadata digest.
#[derive(Debug, PartialEq)]
struct PrepareDraftOperation {
    run: ExportRun,
    record: RepositoryRecord,
    jsonld: String,
    identity: ExportIdentity,
    step: PrepareStep,
}

impl PrepareDraftOperation {
    fn new(
        run: ExportRun,
        record: RepositoryRecord,
        jsonld: String,
        identity: ExportIdentity,
    ) -> Self {
        Self {
            run,
            record,
            jsonld,
            identity,
            step: PrepareStep::Connect,
        }
    }

    fn requested(&self) -> bool {
        self.run.destination.draft_id.is_some() || self.run.destination.published_id.is_some()
    }

    fn same_draft(&self, draft: &Value) -> Result<bool, TransferError> {
        Ok(record_id(draft)? == self.record.id
            && draft["parent"]["id"] == self.record.parent_id
            && draft["is_published"] == false)
    }

    fn checked(&self, fields: &Value, draft: Value) -> Turn<(RepositoryRecord, [u8; 32])> {
        if !self.same_draft(&draft)? {
            return Err(invalid("repository draft identity changed"));
        }
        verify_metadata(fields, &draft)?;
        if !self.requested() && draft["revision_id"].as_u64() != Some(self.record.revision_id) {
            return Err(invalid("repository metadata changed during creation"));
        }
        let mut record = self.record.clone();
        record.revision_id = draft["revision_id"]
            .as_u64()
            .ok_or_else(|| invalid("missing draft revision"))?;
        Ok(Next::Done((record, metadata_digest(&draft))))
    }

    fn fields_for(&self, session: &Session, current: &Value) -> Result<Value, TransferError> {
        let document: Value =
            serde_json::from_str(&self.jsonld).map_err(|_| invalid("invalid crate metadata"))?;
        let overrides: Value = serde_json::from_str(&self.run.destination.metadata_json)
            .map_err(|_| invalid("invalid metadata overrides"))?;
        let mut fields = mapped_fields(session, &document, &overrides, &self.identity)?;
        fields["files"] = json!({"enabled": true});
        // Keeping pids holds the reserved DOI; file access follows the current setting.
        fields["pids"] = current["pids"].clone();
        let mut access = json!({"record": current["access"]["record"].as_str().unwrap_or("public"),
            "files": self.run.file_access()});
        if current["access"]["embargo"].is_object() {
            access["embargo"] = current["access"]["embargo"].clone();
        }
        fields["access"] = access;
        Ok(fields)
    }
}

impl Operation for PrepareDraftOperation {
    type Output = (RepositoryRecord, [u8; 32]);
    type Error = TransferError;

    fn first(&mut self) -> Turn<Self::Output> {
        Ok(Next::Call(
            self.run.connect("personal login missing", true)?,
        ))
    }

    fn next(&mut self, result: IoResult) -> Turn<Self::Output> {
        match std::mem::replace(&mut self.step, PrepareStep::Connect) {
            PrepareStep::Connect => {
                let session = session_of(result)?;
                // The crate must map to a complete record before the draft is read.
                let document: Value = serde_json::from_str(&self.jsonld)
                    .map_err(|_| invalid("invalid crate metadata"))?;
                let overrides: Value = serde_json::from_str(&self.run.destination.metadata_json)
                    .map_err(|_| invalid("invalid metadata overrides"))?;
                mapped_fields(&session, &document, &overrides, &self.identity)?;
                let call = session.get(session.url(&["records", &self.record.id, "draft"])?);
                self.step = PrepareStep::Current(session);
                Ok(Next::Call(call))
            }
            PrepareStep::Current(session) => {
                let current = json_body(result)?;
                if !self.same_draft(&current)? {
                    return Err(invalid("repository draft identity changed"));
                }
                let fields = self.fields_for(&session, &current)?;
                if !self.requested() {
                    return self.checked(&fields, current);
                }
                let revision = current["revision_id"].as_u64();
                if revision == Some(self.record.revision_id) {
                    self.step = PrepareStep::Guard(session, fields);
                    return Ok(Next::Call(self.run.guard()));
                }
                let adopted = revision.is_some_and(|revision| revision > self.record.revision_id)
                    && complete_metadata(&fields["metadata"], &current["metadata"])
                    && complete_fields(&fields["custom_fields"], &current["custom_fields"])
                    && matches_access(&fields["access"], &current);
                if !adopted {
                    return Err(invalid(
                        "draft changed after an ambiguous metadata update; inspect and recover with draft_id",
                    ));
                }
                self.checked(&fields, current)
            }
            PrepareStep::Guard(session, fields) => {
                guarded(result)?;
                let url = session.url(&["records", &self.record.id, "draft"])?;
                let call = session.update(url, fields.clone(), self.record.revision_id);
                self.step = PrepareStep::Updated(fields);
                Ok(Next::Call(call))
            }
            PrepareStep::Updated(fields) => self.checked(&fields, json_body(result)?),
        }
    }
}

#[derive(Debug, PartialEq)]
enum DepositStep {
    Connect,
    Draft(Session),
    Published(Session),
    Files(Session),
    RemoveGuard(Session, String),
    Removed(Session),
    UploadGuard(Session),
    Upload(Session, String),
    Finish(Session),
    Warned(RepositoryRecord),
}

/// Uploads the export's files and finishes the record. On a record an earlier attempt published,
/// a failed check only warns.
#[derive(Debug, PartialEq)]
struct DepositOperation {
    run: ExportRun,
    record: RepositoryRecord,
    artifact: ArtifactRef,
    metadata: [u8; 32],
    uploads: HashSet<String>,
    inspection: Option<Result<ArchiveInspection, TransferError>>,
    draft: Value,
    published: bool,
    checked: bool,
    listing: VecDeque<Value>,
    remote: BTreeMap<String, Value>,
    local: VecDeque<ArchiveEntry>,
    paths: HashSet<String>,
    verified: BTreeMap<String, FileDigests>,
    uploaded: u64,
    step: DepositStep,
}

impl DepositOperation {
    fn new(
        run: ExportRun,
        record: RepositoryRecord,
        artifact: ArtifactRef,
        metadata: [u8; 32],
        uploads: HashSet<String>,
        inspection: Result<ArchiveInspection, TransferError>,
    ) -> Self {
        Self {
            run,
            record,
            artifact,
            metadata,
            uploads,
            inspection: Some(inspection),
            draft: Value::Null,
            published: false,
            checked: false,
            listing: VecDeque::new(),
            remote: BTreeMap::new(),
            local: VecDeque::new(),
            paths: HashSet::new(),
            verified: BTreeMap::new(),
            uploaded: 0,
            step: DepositStep::Connect,
        }
    }

    fn check_identity(&mut self, session: Session, draft: Value) -> Turn<DepositOutput> {
        if record_id(&draft)? != self.record.id
            || draft["is_published"] != self.published
            || draft["parent"]["id"] != self.record.parent_id
        {
            return Err(invalid(
                "export record identity or publication state changed",
            ));
        }
        self.draft = draft;
        self.checked = true;
        if metadata_digest(&self.draft) != self.metadata {
            return Err(invalid("repository metadata changed after preparation"));
        }
        let inspection = self
            .inspection
            .take()
            .ok_or_else(|| invalid("export artifact inspection missing"))??;
        // Only the files the rules' file target takes are uploaded.
        self.local = inspection
            .entries
            .into_iter()
            .filter(|entry| !entry.directory && self.uploads.contains(&entry.path))
            .collect();
        self.paths = self.local.iter().map(|entry| entry.path.clone()).collect();
        let url = match self.published {
            true => session.url(&["records", &self.record.id, "files"])?,
            false => session.url(&["records", &self.record.id, "draft", "files"])?,
        };
        let call = session.get(url);
        self.step = DepositStep::Files(session);
        Ok(Next::Call(call))
    }

    /// A link keeps one draft in step with the dataset, so files the dataset dropped go.
    fn replaces(&self) -> bool {
        self.run.destination.link.is_some() && !self.published
    }

    fn sort_listing(&mut self, session: Session) -> Turn<DepositOutput> {
        while let Some(file) = self.listing.pop_front() {
            let key = file["key"]
                .as_str()
                .ok_or_else(|| invalid("missing repository file key"))?
                .to_string();
            if self.replaces() && !self.paths.contains(&key) {
                // A file this link never pushed was added in the repository.
                let pushed = self
                    .run
                    .destination
                    .link
                    .as_ref()
                    .is_some_and(|target| target.files.iter().any(|pushed| *pushed == key));
                if !pushed {
                    return Err(TransferError::Refused(LinkFailure::RemoteChanged));
                }
                self.step = DepositStep::RemoveGuard(session, key);
                return Ok(Next::Call(self.run.guard()));
            }
            if !self.paths.contains(&key) || self.remote.insert(key, file).is_some() {
                return Err(invalid(
                    "repository draft contains duplicate or unrelated files",
                ));
            }
        }
        self.run.job.progress.set_total(self.paths.len() as u64);
        self.run.job.progress.set_current(0);
        self.next_upload(session)
    }

    fn next_upload(&mut self, session: Session) -> Turn<DepositOutput> {
        if self.local.is_empty() {
            let finish = FinishDepositOperation::new(
                self.run.clone(),
                session.clone(),
                self.record.clone(),
                self.published,
                self.metadata,
                self.verified.clone(),
            );
            self.step = DepositStep::Finish(session);
            return Ok(Next::Call(IoCall::child(finish)));
        }
        if self.published {
            return self.upload(session);
        }
        self.step = DepositStep::UploadGuard(session);
        Ok(Next::Call(self.run.guard()))
    }

    fn upload(&mut self, session: Session) -> Turn<DepositOutput> {
        let entry = self
            .local
            .pop_front()
            .ok_or_else(|| invalid("repository upload entry missing"))?;
        let path = entry.path.clone();
        let upload = UploadEntryOperation::new(
            session.clone(),
            self.record.id.clone(),
            self.artifact.clone(),
            entry,
            self.remote.get(&path).cloned(),
            self.published,
            self.replaces(),
        );
        self.step = DepositStep::Upload(session, path);
        Ok(Next::Call(IoCall::child(upload)))
    }

    fn advance(&mut self, result: IoResult) -> Turn<DepositOutput> {
        match std::mem::replace(&mut self.step, DepositStep::Connect) {
            DepositStep::Connect => {
                let session = session_of(result)?;
                let draft_url = session.url(&["records", &self.record.id, "draft"])?;
                if draft_url.as_str() != self.record.url {
                    return Err(invalid(
                        "repository connector endpoint changed during export",
                    ));
                }
                let call = session.get(draft_url);
                self.step = DepositStep::Draft(session);
                Ok(Next::Call(call))
            }
            DepositStep::Draft(session) => match json_reply(result)? {
                Ok(draft) => self.check_identity(session, draft),
                Err(InvenioError::Status(404)) if self.run.destination.publish => {
                    self.published = true;
                    let call = session.get(session.url(&["records", &self.record.id])?);
                    self.step = DepositStep::Published(session);
                    Ok(Next::Call(call))
                }
                Err(error) => Err(error.into()),
            },
            DepositStep::Published(session) => {
                let record = json_body(result)?;
                self.check_identity(session, record)
            }
            DepositStep::Files(session) => {
                let files = json_body(result)?;
                self.listing = files["entries"]
                    .as_array()
                    .ok_or_else(|| invalid("missing draft files"))?
                    .iter()
                    .cloned()
                    .collect();
                self.sort_listing(session)
            }
            DepositStep::RemoveGuard(session, key) => {
                guarded(result)?;
                let url = session.url(&["records", &self.record.id, "draft", "files", &key])?;
                let call = session.delete(url);
                self.step = DepositStep::Removed(session);
                Ok(Next::Call(call))
            }
            DepositStep::Removed(session) => {
                finished(result)?;
                self.sort_listing(session)
            }
            DepositStep::UploadGuard(session) => {
                guarded(result)?;
                self.upload(session)
            }
            DepositStep::Upload(session, path) => {
                let digests = result.child::<FileDigests, TransferError>()??;
                self.verified.insert(path, digests);
                self.uploaded += 1;
                self.run.job.progress.set_current(self.uploaded);
                self.next_upload(session)
            }
            DepositStep::Finish(_) => {
                let record = result.child::<RepositoryRecord, TransferError>()??;
                let keys = self.verified.keys().cloned().collect();
                Ok(Next::Done((record, keys)))
            }
            DepositStep::Warned(record) => {
                let keys = file_list(&json_body(result)?)?;
                Ok(Next::Done((record, keys)))
            }
        }
    }

    /// The record an earlier attempt published stays; `message` becomes its warning.
    fn warn(&mut self, session: Session, message: String) -> Turn<DepositOutput> {
        let mut record = record_from(&session, &self.draft)?;
        record.warning = Some(message);
        let url = session.url(&["records", &self.record.id, "files"])?;
        self.checked = false;
        self.step = DepositStep::Warned(record);
        Ok(Next::Call(session.get(url)))
    }

    fn session(&self) -> Option<Session> {
        match &self.step {
            DepositStep::Draft(session)
            | DepositStep::Published(session)
            | DepositStep::Files(session)
            | DepositStep::RemoveGuard(session, _)
            | DepositStep::Removed(session)
            | DepositStep::UploadGuard(session)
            | DepositStep::Upload(session, _)
            | DepositStep::Finish(session) => Some(session.clone()),
            DepositStep::Connect | DepositStep::Warned(_) => None,
        }
    }
}

type DepositOutput = (RepositoryRecord, Vec<String>);

impl Operation for DepositOperation {
    type Output = DepositOutput;
    type Error = TransferError;

    fn first(&mut self) -> Turn<DepositOutput> {
        let call = self
            .run
            .connect("a personal repository login is required", true)?;
        Ok(Next::Call(call))
    }

    fn next(&mut self, result: IoResult) -> Turn<DepositOutput> {
        let session = self.session();
        match self.advance(result) {
            // The record was published by an earlier attempt, so a failed check only warns.
            Err(TransferError::Permanent(message)) if self.published && self.checked => {
                match session {
                    Some(session) => self.warn(session, message),
                    None => Err(TransferError::Permanent(message)),
                }
            }
            turn => turn,
        }
    }
}

#[derive(Debug, PartialEq)]
enum UploadStep {
    Hash,
    Removed,
    Commit,
    Started,
    Sent,
}

/// Brings one crate file into the draft: checks the stored bytes, keeps a matching remote file,
/// replaces a differing one on a link, uploads what is missing and commits it.
#[derive(Debug, PartialEq)]
struct UploadEntryOperation {
    session: Session,
    record_id: String,
    artifact: ArtifactRef,
    entry: ArchiveEntry,
    existing: Option<Value>,
    published: bool,
    replace: bool,
    range: std::ops::Range<u64>,
    expected: Option<FileDigests>,
    step: UploadStep,
}

impl UploadEntryOperation {
    fn new(
        session: Session,
        record_id: String,
        artifact: ArtifactRef,
        entry: ArchiveEntry,
        existing: Option<Value>,
        published: bool,
        replace: bool,
    ) -> Self {
        Self {
            session,
            record_id,
            artifact,
            entry,
            existing,
            published,
            replace,
            range: 0..0,
            expected: None,
            step: UploadStep::Hash,
        }
    }

    fn file_url(&self, tail: &[&str]) -> Result<url::Url, TransferError> {
        let mut segments = vec![
            "records",
            &self.record_id,
            "draft",
            "files",
            &self.entry.path,
        ];
        segments.extend_from_slice(tail);
        self.session.url(&segments)
    }

    fn digests(&self) -> Result<&FileDigests, TransferError> {
        self.expected
            .as_ref()
            .ok_or_else(|| invalid("crate file digests missing"))
    }

    fn commit(&mut self) -> Turn<FileDigests> {
        let url = self.file_url(&["commit"])?;
        self.step = UploadStep::Commit;
        Ok(Next::Call(self.session.post(url, None)))
    }

    fn send(&mut self) -> Turn<FileDigests> {
        let url = self.file_url(&["content"])?;
        let size = self.digests()?.size;
        self.step = UploadStep::Sent;
        Ok(Next::Call(self.session.upload(
            url,
            size,
            &self.artifact,
            self.range.clone(),
        )))
    }

    fn after_hash(&mut self) -> Turn<FileDigests> {
        let digests = self.digests()?.clone();
        if let Some(file) = self
            .existing
            .as_ref()
            .filter(|file| file["checksum"].is_string())
        {
            verify_file(file, &digests, false)?;
            if file["status"] == "completed" {
                return Ok(Next::Done(digests));
            }
            if self.published {
                return Err(invalid("published record contains an incomplete file"));
            }
            return self.commit();
        }
        if self.published {
            return Err(invalid("published record is missing a complete crate file"));
        }
        if self.existing.is_some() {
            return self.send();
        }
        let url = self
            .session
            .url(&["records", &self.record_id, "draft", "files"])?;
        self.step = UploadStep::Started;
        let body = json!([{"key": self.entry.path}]);
        Ok(Next::Call(self.session.post(url, Some(body))))
    }
}

impl Operation for UploadEntryOperation {
    type Output = FileDigests;
    type Error = TransferError;

    fn first(&mut self) -> Turn<FileDigests> {
        let entry = &self.entry;
        if entry.compression != ArchiveCompression::Stored
            || entry.compressed_size != entry.uncompressed_size
        {
            return Err(invalid("repository snapshot entry is not stored verbatim"));
        }
        let end = entry
            .data_offset
            .checked_add(entry.uncompressed_size)
            .filter(|end| *end <= self.artifact.size)
            .ok_or_else(|| invalid("repository file exceeds snapshot"))?;
        self.range = entry.data_offset..end;
        Ok(Next::Call(remote_call(
            RemoteCall::HashRange {
                source: self.artifact.location.clone(),
                range: self.range.clone(),
                limit: entry.uncompressed_size,
            },
            self.session.stop(),
        )))
    }

    fn next(&mut self, result: IoResult) -> Turn<FileDigests> {
        match self.step {
            UploadStep::Hash => {
                let RemoteAnswer::Digests(digests) = reply(result)?? else {
                    return Err(invalid("unexpected artifact hash answer"));
                };
                if digests.size != self.entry.uncompressed_size
                    || digests.crc32 != self.entry.crc32.to_be_bytes()
                {
                    return Err(invalid("crate file differs from snapshot"));
                }
                let replaced = self.replace
                    && self.existing.as_ref().is_some_and(|file| {
                        file["checksum"].is_string() && verify_file(file, &digests, false).is_err()
                    });
                self.expected = Some(digests);
                if replaced {
                    let url = self.file_url(&[])?;
                    self.step = UploadStep::Removed;
                    return Ok(Next::Call(self.session.delete(url)));
                }
                self.after_hash()
            }
            UploadStep::Removed => {
                finished(result)?;
                self.existing = None;
                self.after_hash()
            }
            UploadStep::Started => {
                json_body(result)?;
                self.send()
            }
            UploadStep::Sent => {
                let RemoteAnswer::Digests(sent) = reply(result)?? else {
                    return Err(invalid("unexpected upload answer"));
                };
                if sent.blake3 != self.digests()?.blake3 {
                    return Err(invalid("crate file changed while uploading"));
                }
                self.commit()
            }
            UploadStep::Commit => {
                let digests = self.digests()?.clone();
                verify_file(&json_body(result)?, &digests, true)?;
                Ok(Next::Done(digests))
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum FinishStep {
    Current,
    Files(Value),
    Guard(Value),
    Community(Value),
    ReviewTarget,
    ReviewRequest,
    Submitted,
    ReviewDraft,
    Publish,
    PublishedFiles(RepositoryRecord),
}

/// Checks the finished draft and publishes it or submits it to its community review.
#[derive(Debug, PartialEq)]
struct FinishDepositOperation {
    run: ExportRun,
    session: Session,
    record: RepositoryRecord,
    published: bool,
    metadata: [u8; 32],
    files: BTreeMap<String, FileDigests>,
    step: FinishStep,
}

impl FinishDepositOperation {
    fn new(
        run: ExportRun,
        session: Session,
        record: RepositoryRecord,
        published: bool,
        metadata: [u8; 32],
        files: BTreeMap<String, FileDigests>,
    ) -> Self {
        Self {
            run,
            session,
            record,
            published,
            metadata,
            files,
            step: FinishStep::Current,
        }
    }

    fn id(&self) -> &str {
        &self.record.id
    }

    fn files_call(&self, published: bool) -> Result<IoCall, TransferError> {
        let url = match published {
            true => self.session.url(&["records", self.id(), "files"])?,
            false => self
                .session
                .url(&["records", self.id(), "draft", "files"])?,
        };
        Ok(self.session.get(url))
    }

    fn publish(&mut self) -> Turn<RepositoryRecord> {
        let url = self
            .session
            .url(&["records", self.id(), "draft", "actions", "publish"])?;
        self.step = FinishStep::Publish;
        Ok(Next::Call(self.session.post(url, None)))
    }

    fn submit_review(&mut self, current: &Value, community: &str) -> Turn<RepositoryRecord> {
        // A submission still open stays as it is.
        if current["parent"]["review"]["status"] == "submitted" {
            return self.read_review_draft();
        }
        let url = self.session.url(&["communities", community])?;
        self.step = FinishStep::ReviewTarget;
        Ok(Next::Call(self.session.get(url)))
    }

    fn read_review_draft(&mut self) -> Turn<RepositoryRecord> {
        let url = self.session.url(&["records", self.id(), "draft"])?;
        self.step = FinishStep::ReviewDraft;
        Ok(Next::Call(self.session.get(url)))
    }
}

impl Operation for FinishDepositOperation {
    type Output = RepositoryRecord;
    type Error = TransferError;

    fn first(&mut self) -> Turn<RepositoryRecord> {
        let url = match self.published {
            true => self.session.url(&["records", self.id()])?,
            false => self.session.url(&["records", self.id(), "draft"])?,
        };
        Ok(Next::Call(self.session.get(url)))
    }

    fn next(&mut self, result: IoResult) -> Turn<RepositoryRecord> {
        match std::mem::replace(&mut self.step, FinishStep::Publish) {
            FinishStep::Current => {
                let current = json_body(result)?;
                if metadata_digest(&current) != self.metadata {
                    return Err(invalid("repository metadata changed before publication"));
                }
                if current["parent"]["id"] != self.record.parent_id {
                    return Err(invalid("repository parent changed"));
                }
                if !self.run.destination.public_files && current["access"]["files"] != "restricted"
                {
                    return Err(invalid(
                        "repository file access is more public than requested",
                    ));
                }
                self.step = FinishStep::Files(current);
                Ok(Next::Call(self.files_call(self.published)?))
            }
            FinishStep::Files(current) => {
                verify_files(&json_body(result)?, &self.files)?;
                if !self.run.destination.publish || self.published {
                    return Ok(Next::Done(record_from(&self.session, &current)?));
                }
                self.step = FinishStep::Guard(current);
                Ok(Next::Call(self.run.guard()))
            }
            FinishStep::Guard(current) => {
                guarded(result)?;
                // A record's first version needs the connector community's review.
                if current["versions"]["index"] != 1
                    || !supports(RepositoryConnectorKind::Invenio, Action::Review)
                {
                    return self.publish();
                }
                self.step = FinishStep::Community(current);
                Ok(Next::Call(IoCall::child(ConnectorViewOperation::new(
                    self.run.destination.group_id,
                    self.run.destination.connector_id,
                ))))
            }
            FinishStep::Community(current) => {
                let view = result.child::<ConnectorView, TransferError>()??;
                let community = view
                    .connector
                    .public_config
                    .get(INVENIO_COMMUNITY)
                    .filter(|community| !community.is_empty());
                match community {
                    Some(community) => self.submit_review(&current, community),
                    None => self.publish(),
                }
            }
            FinishStep::ReviewTarget => {
                let found = json_body(result)?;
                let id = found["id"]
                    .as_str()
                    .ok_or_else(|| invalid("community has no id"))?;
                let request =
                    json!({"receiver": {"community": id}, "type": "community-submission"});
                let url = self
                    .session
                    .url(&["records", self.id(), "draft", "review"])?;
                self.step = FinishStep::ReviewRequest;
                Ok(Next::Call(self.session.put(url, request)))
            }
            FinishStep::ReviewRequest => {
                json_body(result)?;
                let url = self.session.url(&[
                    "records",
                    self.id(),
                    "draft",
                    "actions",
                    "submit-review",
                ])?;
                self.step = FinishStep::Submitted;
                Ok(Next::Call(self.session.post(url, None)))
            }
            FinishStep::Submitted => {
                json_body(result)?;
                self.read_review_draft()
            }
            FinishStep::ReviewDraft => {
                let mut record = record_from(&self.session, &json_body(result)?)?;
                record.in_review = true;
                Ok(Next::Done(record))
            }
            FinishStep::Publish => {
                let answer = json_body(result)?;
                if record_id(&answer)? != self.record.id || answer["is_published"] != true {
                    return Err(invalid("repository did not confirm publication"));
                }
                // Publishing cannot be undone, so later mismatches become a warning.
                let mut published = record_from(&self.session, &answer)?;
                published.warning = if metadata_digest(&answer) != self.metadata {
                    Some("published metadata changed".into())
                } else if answer["parent"]["id"] != self.record.parent_id {
                    Some("published parent changed".into())
                } else if !self.run.destination.public_files
                    && answer["access"]["files"] != "restricted"
                {
                    Some("published file access is more public than requested".into())
                } else {
                    None
                };
                if published.warning.is_some() {
                    return Ok(Next::Done(published));
                }
                self.step = FinishStep::PublishedFiles(published);
                Ok(Next::Call(self.files_call(true)?))
            }
            FinishStep::PublishedFiles(mut published) => {
                let checked = json_body(result).and_then(|files| verify_files(&files, &self.files));
                published.warning = match checked {
                    Ok(()) => None,
                    Err(error @ (TransferError::Cancelled | TransferError::Interrupted)) => {
                        return Err(error);
                    }
                    Err(error) => Some(error.to_string()),
                };
                Ok(Next::Done(published))
            }
        }
    }
}

/// The record's page for people, kept only on the repository's own origin.
fn page_url(session: &Session, record: &Value) -> Option<String> {
    let page = url::Url::parse(record["links"]["self_html"].as_str()?).ok()?;
    let endpoint = url::Url::parse(session.endpoint()).ok()?;
    (page.origin() == endpoint.origin() && page.username().is_empty() && page.password().is_none())
        .then(|| page.to_string())
}

#[cfg(test)]
#[path = "export_tests.rs"]
mod tests;
