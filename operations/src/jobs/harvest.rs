//! Runs the harvest job that pages an OAI-PMH source and stores records as metadata documents.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{HashSet, VecDeque};
use std::time::Duration;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, NetEffect};
use aruna_core::events::{Event, NetEvent, StorageEvent};
use aruna_core::keyspaces::PENDING_PROJECTION_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::repository::remote::{
    HarvestWrite, HarvestWriteFailure, NodeScope, RemoteAnswer, RemoteCall, RemoteFailure,
    StopReason,
};
use aruna_core::rows::{event, page};
use aruna_core::structs::execution::harvest::{
    HarvestCursor, HarvestJobSpec, HarvestProvenance, HarvestRecordState, HarvestSource,
    IncomingRecord, ProvenanceDecision, RepositoryConnector, RepositoryConnectorKind,
    provenance_decision,
};
use aruna_core::structs::execution::job::{JobError, JobResultPayload};
use aruna_core::structs::identity::auth::{Actor, AuthContext};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::structured_id::StructuredId;
use aruna_core::telemetry::record_stage;
use byteview::ByteView;
use tracing::warn;
use ulid::Ulid;

use super::repository::link_store::time_of;
use super::repository::{JobScope, remote_answer, remote_call};
use crate::auth::error::{AuthorizeError, PolicyEnforcementError};
use crate::auth::request_policy::PolicyRequestExtras;
use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::forward::authorize::{AuthorizeCreateOperation, CreateScope};
use crate::harvest::oai_pmh::mapping::dc_to_jsonld;
use crate::harvest::oai_pmh::parse::{
    OaiPage, OaiParseError, OaiRecord, parse_datestamp_ms, parse_granularity, parse_list_page,
};
use crate::harvest::oai_pmh::request::{format_window, identify_url, list_records_url};
use crate::harvest::repository::{
    StorageReadError, parse_connector_read, parse_provenance_read, parse_source_read,
    read_connector_effect, read_provenance_effect, read_source_effect, write_provenance_effect,
    write_source_effect,
};
use crate::harvest::target_path::{HARVEST_PATH_BYTES, normalize_target_prefix};
use crate::jobs::executor::JobRunOutcome;
use crate::metadata::create_document::mint_document_for;
use crate::metadata::repository::{parse_registry_read, read_document_registry};

/// Bound on resumption-token paging so a broken provider cannot loop forever.
/// Operationally generous at a typical page size, and small enough that a
/// pathological provider cannot hold a worker for the life of the node.
const MAX_HARVEST_PAGES: u32 = 10_000;
/// Total wall clock one harvest run may spend paging.
const HARVEST_RUN_DEADLINE: Duration = Duration::from_secs(1800);

/// What one harvest run did.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct HarvestCounts {
    minted: u64,
    updated: u64,
    tombstoned: u64,
    skipped: u64,
}

impl HarvestCounts {
    fn add(&mut self, applied: RecordApplied) {
        match applied {
            RecordApplied::Minted => self.minted += 1,
            RecordApplied::Updated => self.updated += 1,
            RecordApplied::Tombstoned => self.tombstoned += 1,
            RecordApplied::Skipped => self.skipped += 1,
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum HarvestFailure {
    Cancelled,
    Interrupted,
    Job(JobError),
}

impl From<IoError> for HarvestFailure {
    fn from(error: IoError) -> Self {
        retryable(format!("harvest storage: {error}"))
    }
}

/// The job outcome of a finished harvest run.
pub fn harvest_outcome(result: Result<HarvestCounts, HarvestFailure>) -> JobRunOutcome {
    match result {
        Ok(counts) => JobRunOutcome::Succeeded(JobResultPayload::Harvest {
            minted: counts.minted,
            updated: counts.updated,
            tombstoned: counts.tombstoned,
            skipped: counts.skipped,
        }),
        Err(HarvestFailure::Cancelled) => JobRunOutcome::Cancelled,
        Err(HarvestFailure::Interrupted) => JobRunOutcome::Interrupted,
        Err(HarvestFailure::Job(error)) => JobRunOutcome::Failed(error),
    }
}

type Turn<T> = Result<Next<T>, HarvestFailure>;

/// What runs after the source row was written.
#[derive(Debug, PartialEq)]
enum AfterSave {
    Listing,
    NextPage,
    Finish,
    Fail(HarvestFailure),
    /// The provider disowned its token: restart the window once, else fail with this error.
    Restart(OaiParseError),
}

#[derive(Debug, PartialEq)]
enum HarvestStep {
    Source,
    Connector,
    Identify,
    Save(AfterSave),
    Gate,
    Fetch,
    RecordGate,
    Record,
}

/// One harvest: pages through OAI-PMH ListRecords, applies each record through the metadata
/// write seam with the source's group authority, and advances the cursor. Idempotent by harvest
/// provenance, so a fenced re-run re-applies nothing.
#[derive(Debug, PartialEq)]
pub struct HarvestJobOperation {
    scope: NodeScope,
    job: JobScope,
    spec: HarvestJobSpec,
    source: Option<HarvestSource>,
    endpoint: String,
    actor: Option<Actor>,
    original_last: u64,
    from: Option<String>,
    token: Option<String>,
    resumed: Option<String>,
    overall_max: u64,
    counts: HarvestCounts,
    // One restart per run at most: a provider that keeps rejecting its own tokens must not be
    // able to loop the listing back to the start forever.
    restarted: bool,
    warned_datestamp: bool,
    seen_tokens: HashSet<[u8; 32]>,
    page_index: u32,
    records: VecDeque<OaiRecord>,
    page_size: u64,
    page_token: Option<String>,
    started_ms: Option<u64>,
    now_ms: u64,
    step: HarvestStep,
}

impl HarvestJobOperation {
    pub fn new(scope: NodeScope, job: JobScope, spec: HarvestJobSpec) -> Self {
        Self {
            scope,
            job,
            spec,
            source: None,
            endpoint: String::new(),
            actor: None,
            original_last: 0,
            from: None,
            token: None,
            resumed: None,
            overall_max: 0,
            counts: HarvestCounts::default(),
            restarted: false,
            warned_datestamp: false,
            seen_tokens: HashSet::new(),
            page_index: 0,
            records: VecDeque::new(),
            page_size: 0,
            page_token: None,
            started_ms: None,
            now_ms: 0,
            step: HarvestStep::Source,
        }
    }

    fn source(&self) -> Result<&HarvestSource, HarvestFailure> {
        self.source
            .as_ref()
            .ok_or_else(|| permanent("harvest source not found"))
    }

    fn checkpoint(&self) -> IoCall {
        remote_call(RemoteCall::Checkpoint, Some(self.job.stop.clone()))
    }

    fn fetch(&self, url: url::Url) -> IoCall {
        remote_call(RemoteCall::OaiFetch { url }, Some(self.job.stop.clone()))
    }

    fn connector(&mut self, connector: RepositoryConnector) -> Turn<HarvestCounts> {
        if connector.kind != RepositoryConnectorKind::OaiPmh {
            return Err(permanent(
                "harvest requires an OAI-PMH repository connector",
            ));
        }
        let (_, realm_id) = self
            .scope
            .node
            .ok_or_else(|| permanent("net plane unavailable for harvest"))?;
        if !self.scope.has_blob {
            return Err(permanent("blob handle unavailable for harvest"));
        }
        let source = self.source()?;
        let actor = Actor {
            node_id: self.job.owner_node,
            user_id: source.created_by,
            realm_id,
        };
        let original_last = source
            .cursor
            .as_ref()
            .map_or(0, |cursor| cursor.last_datestamp_ms);
        let needs_granularity = source.granularity.is_none();
        self.actor = Some(actor);
        self.original_last = original_last;
        self.endpoint = connector.endpoint;
        if needs_granularity {
            // `Identify` is a hint, not a gate: an unreachable or unparseable answer leaves
            // this run on the baseline and is rediscovered next run.
            if let Ok(url) = identify_url(&self.endpoint) {
                self.step = HarvestStep::Identify;
                return Ok(Next::Call(self.fetch(url)));
            }
        }
        self.begin_listing()
    }

    fn begin_listing(&mut self) -> Turn<HarvestCounts> {
        let source = self.source()?;
        let from = format_window(self.original_last, source.granularity.unwrap_or_default());
        let token = source
            .cursor
            .as_ref()
            .and_then(|cursor| cursor.resumption_token.clone());
        self.from = from;
        self.token = token;
        self.overall_max = self.original_last;
        self.gate()
    }

    fn gate(&mut self) -> Turn<HarvestCounts> {
        self.step = HarvestStep::Gate;
        Ok(Next::Call(self.checkpoint()))
    }

    fn save_source(&mut self, source: &HarvestSource, then: AfterSave) -> Turn<HarvestCounts> {
        let mut updated = source.clone();
        updated.updated_at = time_of(self.now_ms);
        let effect = write_source_effect(&updated, None)
            .map_err(|error| permanent(format!("harvest source encode: {error}")))?;
        self.step = HarvestStep::Save(then);
        Ok(Next::Call(effect.into()))
    }

    fn save_cursor(
        &mut self,
        last_datestamp_ms: u64,
        resumption_token: Option<String>,
        then: AfterSave,
    ) -> Turn<HarvestCounts> {
        let mut updated = self.source()?.clone();
        updated.cursor = Some(HarvestCursor {
            last_datestamp_ms,
            resumption_token,
        });
        self.save_source(&updated, then)
    }

    fn request_page(&mut self) -> Turn<HarvestCounts> {
        let elapsed = self
            .now_ms
            .saturating_sub(self.started_ms.unwrap_or(self.now_ms));
        if self.page_index + 1 == MAX_HARVEST_PAGES
            || elapsed > HARVEST_RUN_DEADLINE.as_millis() as u64
        {
            // Budget exhaustion is not a defect in the source: keep the position so the next
            // run continues where this one stopped.
            let token = self.token.clone();
            let failure = retryable("harvest exhausted its paging or time budget");
            return self.save_cursor(self.original_last, token, AfterSave::Fail(failure));
        }
        self.resumed = self.token.clone();
        let source = self.source()?;
        let url = list_records_url(
            &self.endpoint,
            &source.selector,
            self.from.as_deref(),
            self.resumed.as_deref(),
        )
        .map_err(|error| permanent(format!("invalid OAI endpoint URL: {error}")))?;
        self.step = HarvestStep::Fetch;
        Ok(Next::Call(self.fetch(url)))
    }

    fn take_page(&mut self, body: &str) -> Turn<HarvestCounts> {
        let page = match parse_list_page(body) {
            Ok(page) => page,
            Err(error) if self.resumed.is_some() => {
                return self.save_cursor(self.original_last, None, AfterSave::Restart(error));
            }
            Err(error) => return Err(classify_parse(error)),
        };
        let OaiPage {
            records,
            resumption_token,
        } = page;
        self.page_size = records.len() as u64;
        self.records = records.into();
        self.page_token = resumption_token;
        self.next_record()
    }

    fn next_record(&mut self) -> Turn<HarvestCounts> {
        if self.records.is_empty() {
            return self.end_page();
        }
        self.step = HarvestStep::RecordGate;
        Ok(Next::Call(self.checkpoint()))
    }

    fn apply_next(&mut self) -> Turn<HarvestCounts> {
        let record = self
            .records
            .pop_front()
            .ok_or_else(|| retryable("harvest record missing"))?;
        // An unparseable datestamp harvests as epoch zero; one warning per run names the
        // offending value without flooding a large listing.
        let datestamp_ms = parse_datestamp_ms(&record.header.datestamp).unwrap_or_else(|| {
            if !self.warned_datestamp {
                self.warned_datestamp = true;
                warn!(
                    datestamp = %record.header.datestamp,
                    identifier = %record.header.identifier,
                    "Unparseable OAI datestamp; harvesting the record as epoch zero"
                );
            }
            0
        });
        self.overall_max = self.overall_max.max(datestamp_ms);
        let actor = self
            .actor
            .clone()
            .ok_or_else(|| permanent("net plane unavailable for harvest"))?;
        let apply = ApplyRecordOperation::new(self.source()?.clone(), actor, record, datestamp_ms);
        self.step = HarvestStep::Record;
        Ok(Next::Call(IoCall::child(apply)))
    }

    fn end_page(&mut self) -> Turn<HarvestCounts> {
        self.job.progress.advance(self.page_size);
        self.token = self.page_token.take();
        let Some(token) = self.token.clone() else {
            // Complete: advance the window so the next harvest is incremental.
            return self.save_cursor(self.overall_max, None, AfterSave::Finish);
        };
        if !self
            .seen_tokens
            .insert(*blake3::hash(token.as_bytes()).as_bytes())
        {
            let failure = retryable("OAI provider returned a resumption token twice");
            return self.save_cursor(self.original_last, None, AfterSave::Fail(failure));
        }
        // Mid-list: keep the window fixed and store the token so a re-run resumes.
        self.save_cursor(self.original_last, Some(token), AfterSave::NextPage)
    }

    fn after_save(&mut self, then: AfterSave) -> Turn<HarvestCounts> {
        match then {
            AfterSave::Listing => self.begin_listing(),
            AfterSave::NextPage => {
                self.page_index += 1;
                self.gate()
            }
            AfterSave::Finish => Ok(Next::Done(std::mem::take(&mut self.counts))),
            AfterSave::Fail(failure) => Err(failure),
            AfterSave::Restart(error) => {
                if !self.restarted && is_bad_token(&error) {
                    self.restarted = true;
                    self.token = None;
                    // A restart is a fresh listing: the provider may legitimately hand back
                    // the same token names again.
                    self.seen_tokens.clear();
                    self.page_index += 1;
                    return self.gate();
                }
                Err(retryable(format!(
                    "OAI resumption token rejected, listing restarts next run: {error}"
                )))
            }
        }
    }
}

/// The text of a fetch, or the failure it ended with.
fn fetched(result: IoResult) -> Result<Result<String, HarvestFailure>, HarvestFailure> {
    Ok(match remote_answer(result)? {
        Ok(RemoteAnswer::Text(body)) => Ok(body),
        Ok(_) => Err(retryable("OAI fetch: unexpected answer")),
        Err(failure) => Err(remote_failure(failure)),
    })
}

fn passed(result: IoResult) -> Result<(), HarvestFailure> {
    match remote_answer(result)? {
        Ok(_) => Ok(()),
        Err(failure) => Err(remote_failure(failure)),
    }
}

fn remote_failure(failure: RemoteFailure) -> HarvestFailure {
    match failure {
        RemoteFailure::Stopped(StopReason::Cancelled) => HarvestFailure::Cancelled,
        RemoteFailure::Stopped(StopReason::Interrupted) => HarvestFailure::Interrupted,
        RemoteFailure::Permanent(message) => permanent(message),
        RemoteFailure::Retryable(message) => retryable(message),
        RemoteFailure::Invenio(error) => retryable(error.to_string()),
    }
}

impl Operation for HarvestJobOperation {
    type Output = HarvestCounts;
    type Error = HarvestFailure;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
        self.started_ms.get_or_insert(now_ms);
    }

    fn first(&mut self) -> Turn<HarvestCounts> {
        Ok(Next::Call(
            read_source_effect(self.spec.group_id, self.spec.source_id, None).into(),
        ))
    }

    fn next(&mut self, result: IoResult) -> Turn<HarvestCounts> {
        match std::mem::replace(&mut self.step, HarvestStep::Gate) {
            HarvestStep::Source => {
                let source = parse_source_read(event(result)?)
                    .map_err(read_failure)?
                    .ok_or_else(|| permanent("harvest source not found"))?;
                let call = read_connector_effect(source.group_id, source.connector_id, None);
                self.source = Some(source);
                self.step = HarvestStep::Connector;
                Ok(Next::Call(call.into()))
            }
            HarvestStep::Connector => {
                let connector = parse_connector_read(event(result)?)
                    .map_err(read_failure)?
                    .ok_or_else(|| permanent("repository connector not found"))?;
                self.connector(connector)
            }
            HarvestStep::Identify => {
                let granularity = fetched(result)?
                    .ok()
                    .and_then(|body| parse_granularity(&body));
                let Some(granularity) = granularity else {
                    return self.begin_listing();
                };
                let source = self
                    .source
                    .as_mut()
                    .ok_or_else(|| permanent("harvest source not found"))?;
                source.granularity = Some(granularity);
                let source = source.clone();
                self.save_source(&source, AfterSave::Listing)
            }
            HarvestStep::Save(then) => {
                expect_write(event(result)?)?;
                self.after_save(then)
            }
            HarvestStep::Gate => {
                passed(result)?;
                self.request_page()
            }
            HarvestStep::Fetch => {
                let body = fetched(result)??;
                self.take_page(&body)
            }
            HarvestStep::RecordGate => {
                passed(result)?;
                self.apply_next()
            }
            HarvestStep::Record => {
                let applied = result.child::<RecordApplied, HarvestFailure>()??;
                self.counts.add(applied);
                self.next_record()
            }
        }
    }
}

fn is_bad_token(error: &OaiParseError) -> bool {
    matches!(error, OaiParseError::Protocol { code, .. } if code == "badResumptionToken")
}

/// A malformed body is upstream junk that a retry can clear; an OAI protocol
/// rejection of a well-formed request is a configuration fault that will not.
fn classify_parse(error: OaiParseError) -> HarvestFailure {
    match error {
        OaiParseError::Xml(_) => retryable(format!("OAI response: {error}")),
        OaiParseError::Protocol { .. } => permanent(format!("OAI response: {error}")),
    }
}

/// How one source record changed the harvest.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RecordApplied {
    Minted,
    Updated,
    Tombstoned,
    Skipped,
}

#[derive(Debug, PartialEq)]
enum ApplyStep {
    Provenance,
    MintConfig,
    MintPending(Ulid),
    CreateAccess(Ulid),
    Create(Ulid),
    CreateStored(Ulid),
    CreateUpdate,
    UpdateAccess(Ulid),
    UpdateStored(Ulid),
    UpdatePending(Ulid),
    Update(Ulid, bool),
    TombstoneAccess(Ulid),
    TombstoneStored(Ulid),
    TombstonePending(Ulid),
    Delete(Ulid),
    Final(RecordApplied),
}

/// Applies one source record: its provenance decides, the metadata seam writes, and the
/// provenance row records the outcome.
#[derive(Debug, PartialEq)]
pub(crate) struct ApplyRecordOperation {
    source: HarvestSource,
    actor: Actor,
    record: OaiRecord,
    datestamp_ms: u64,
    row: Option<HarvestProvenance>,
    now_ms: u64,
    access_started_ms: u64,
    step: ApplyStep,
}

type ApplyTurn = Turn<RecordApplied>;

impl ApplyRecordOperation {
    pub(crate) fn new(
        source: HarvestSource,
        actor: Actor,
        record: OaiRecord,
        datestamp_ms: u64,
    ) -> Self {
        Self {
            source,
            actor,
            record,
            datestamp_ms,
            row: None,
            now_ms: 0,
            access_started_ms: 0,
            step: ApplyStep::Provenance,
        }
    }

    fn row(&mut self) -> Result<&mut HarvestProvenance, HarvestFailure> {
        self.row
            .as_mut()
            .ok_or_else(|| retryable("harvest provenance row missing"))
    }

    fn owner(&self) -> AuthContext {
        AuthContext {
            user_id: self.source.created_by,
            realm_id: self.actor.realm_id,
            path_restrictions: None,
            session: None,
        }
    }

    fn path(&self) -> Result<String, HarvestFailure> {
        harvest_document_path(&self.source.target_prefix, &self.record.header.identifier)
    }

    fn write_row(&mut self, then: ApplyStep) -> ApplyTurn {
        let row = self.row()?.clone();
        let effect = write_provenance_effect(&row, None)
            .map_err(|error| permanent(format!("provenance encode: {error}")))?;
        self.step = then;
        Ok(Next::Call(effect.into()))
    }

    fn finish_live(&mut self, document_id: Ulid, applied: RecordApplied) -> ApplyTurn {
        let row = self.row()?;
        row.meta_resource_id = document_id;
        row.state = HarvestRecordState::Live;
        self.write_row(ApplyStep::Final(applied))
    }

    /// Allocates a document id through the realm config; see [`ApplyStep::MintPending`].
    fn mint(&mut self) -> ApplyTurn {
        self.path()?;
        self.step = ApplyStep::MintConfig;
        Ok(Next::Call(IoCall::child(GetConfigOperation::new(
            self.actor.realm_id,
        ))))
    }

    /// Checks the source owner as a direct metadata create of the record's document path would.
    fn access(&mut self, document_id: Ulid) -> Result<IoCall, HarvestFailure> {
        let path = self.path()?;
        self.access_started_ms = self.now_ms;
        Ok(IoCall::child(AuthorizeCreateOperation::new(
            self.actor.realm_id,
            &self.owner(),
            PolicyRequestExtras::default(),
            self.source.group_id,
            &path,
            document_id,
            CreateScope::Group,
        )))
    }

    /// How long the running [`Self::access`] check has taken on the runner's clock.
    fn access_elapsed(&self) -> Duration {
        Duration::from_millis(self.now_ms.saturating_sub(self.access_started_ms))
    }

    /// The end of an [`Self::access`] check; a refusal names the record's path.
    fn access_result(&self, result: IoResult) -> Result<(), HarvestFailure> {
        record_stage("permission", self.access_elapsed());
        match result.child::<(), AuthorizeError>()? {
            Ok(()) => Ok(()),
            Err(
                AuthorizeError::PermissionDenied
                | AuthorizeError::Policy(PolicyEnforcementError::Denied { .. }),
            ) => Err(permanent(format!(
                "harvest write to [{}] is not permitted",
                self.path()?
            ))),
            Err(error) => Err(retryable(format!("harvest permission check: {error}"))),
        }
    }

    fn create_access(&mut self, document_id: Ulid) -> ApplyTurn {
        let call = self.access(document_id)?;
        self.step = ApplyStep::CreateAccess(document_id);
        Ok(Next::Call(call))
    }

    fn routed(&self, write: HarvestWrite) -> IoCall {
        IoCall::Effect(Box::new(Effect::Net(NetEffect::HarvestWrite(Box::new(
            write,
        )))))
    }

    fn update_call(&self, document_id: Ulid, record: Option<MetadataRegistryRecord>) -> IoCall {
        self.routed(HarvestWrite::Update {
            actor: self.actor.clone(),
            owner: self.owner(),
            record: record.map(Box::new),
            document_id,
            jsonld: dc_to_jsonld(&self.record),
        })
    }

    fn registry_call(document_id: Ulid) -> IoCall {
        read_document_registry(document_id, None).into()
    }

    fn pending_call(document_id: Ulid) -> IoCall {
        // An empty registry read is only evidence of absence once nothing committed for the
        // document is still queued for projection here.
        IoCall::Page {
            space: PENDING_PROJECTION_KEYSPACE.to_string(),
            prefix: ByteView::from(document_id.to_bytes().to_vec()),
            after: None,
            limit: 1,
        }
    }

    fn delete_call(&self, document_id: Ulid, record: Option<MetadataRegistryRecord>) -> IoCall {
        self.routed(HarvestWrite::Delete {
            actor: self.actor.clone(),
            owner: self.owner(),
            record: record.map(Box::new),
            document_id,
        })
    }

    fn decide(&mut self, existing: Option<HarvestProvenance>) -> ApplyTurn {
        let incoming = IncomingRecord {
            datestamp_ms: self.datestamp_ms,
            deleted: self.record.header.deleted,
        };
        let decision = provenance_decision(existing.as_ref(), &incoming);
        let mut row = HarvestProvenance {
            group_id: self.source.group_id,
            namespace: self.source.namespace.clone(),
            source_record_id: self.record.header.identifier.clone(),
            meta_resource_id: Ulid::nil(),
            version: next_version(existing.as_ref()),
            source_datestamp_ms: self.datestamp_ms,
            state: HarvestRecordState::PendingCreate,
            predecessors: existing
                .as_ref()
                .map(|provenance| provenance.predecessors.clone())
                .unwrap_or_default(),
        };
        match decision {
            ProvenanceDecision::Skip => Ok(Next::Done(RecordApplied::Skipped)),
            ProvenanceDecision::Mint => {
                self.row = Some(row);
                self.mint()
            }
            ProvenanceDecision::Revive { predecessor } => {
                row.predecessors.push(predecessor);
                self.row = Some(row);
                self.mint()
            }
            ProvenanceDecision::ResumeCreate { meta_resource_id } => {
                row.meta_resource_id = meta_resource_id;
                let stored = existing.map_or(0, |provenance| provenance.source_datestamp_ms);
                row.source_datestamp_ms = self.datestamp_ms.max(stored);
                self.row = Some(row);
                self.create_access(meta_resource_id)
            }
            ProvenanceDecision::Update { meta_resource_id } => {
                self.row = Some(row);
                let call = self.access(meta_resource_id)?;
                self.step = ApplyStep::UpdateAccess(meta_resource_id);
                Ok(Next::Call(call))
            }
            ProvenanceDecision::Tombstone { meta_resource_id } => {
                self.row = Some(row);
                let call = self.access(meta_resource_id)?;
                self.step = ApplyStep::TombstoneAccess(meta_resource_id);
                Ok(Next::Call(call))
            }
        }
    }
}

fn stored_record(result: IoResult) -> Result<Option<MetadataRegistryRecord>, HarvestFailure> {
    parse_registry_read(event(result)?)
        .map_err(|error| retryable(format!("harvest record read: {error:?}")))
}

fn confirm_absent(result: IoResult, document_id: Ulid) -> Result<(), HarvestFailure> {
    let (rows, _) = page(result)
        .map_err(|error| retryable(format!("harvest pending projection scan: {error}")))?;
    if rows.is_empty() {
        return Ok(());
    }
    Err(retryable(format!(
        "harvest waits for the pending registry projection of {document_id}"
    )))
}

fn routed_result(result: IoResult) -> Result<Result<(), HarvestWriteFailure>, HarvestFailure> {
    match event(result)? {
        Event::Net(NetEvent::HarvestWrite(answer)) => Ok(answer),
        other => Err(retryable(format!(
            "harvest write: unexpected {}",
            other.label()
        ))),
    }
}

impl Operation for ApplyRecordOperation {
    type Output = RecordApplied;
    type Error = HarvestFailure;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> ApplyTurn {
        let identifier = &self.record.header.identifier;
        if identifier.is_empty() {
            return Ok(Next::Done(RecordApplied::Skipped));
        }
        Ok(Next::Call(
            read_provenance_effect(
                self.source.group_id,
                &self.source.namespace,
                identifier,
                None,
            )
            .into(),
        ))
    }

    fn next(&mut self, result: IoResult) -> ApplyTurn {
        match std::mem::replace(&mut self.step, ApplyStep::Provenance) {
            ApplyStep::Provenance => {
                let existing = parse_provenance_read(event(result)?).map_err(read_failure)?;
                self.decide(existing)
            }
            // A pending row before the create leaves a retry the same id, so a replay converges
            // on one document instead of orphaning one per attempt.
            ApplyStep::MintConfig => {
                let config = result
                    .child::<RealmConfigDocument, GetConfigError>()?
                    .map_err(|error| retryable(format!("harvest mint document id: {error}")))?;
                let path = self.path()?;
                let document_id =
                    mint_document_for(&config, &self.actor, self.source.group_id, &path, true)
                        .map_err(|error| retryable(format!("harvest mint document id: {error}")))?
                        .as_ulid();
                let row = self.row()?;
                row.meta_resource_id = document_id;
                row.state = HarvestRecordState::PendingCreate;
                self.write_row(ApplyStep::MintPending(document_id))
            }
            ApplyStep::MintPending(document_id) => {
                expect_write(event(result)?)?;
                self.create_access(document_id)
            }
            ApplyStep::CreateAccess(document_id) => {
                self.access_result(result)?;
                let write = HarvestWrite::Create {
                    actor: self.actor.clone(),
                    owner: self.owner(),
                    group_id: self.source.group_id,
                    document_id,
                    document_path: self.path()?,
                    jsonld: dc_to_jsonld(&self.record),
                };
                self.step = ApplyStep::Create(document_id);
                Ok(Next::Call(self.routed(write)))
            }
            ApplyStep::Create(document_id) => match routed_result(result)? {
                Ok(()) => self.finish_live(document_id, RecordApplied::Minted),
                // A create the pending identity already committed under a different payload:
                // the id is resolved, and the newer content lands as an update.
                Err(HarvestWriteFailure::AlreadyExists) => {
                    self.step = ApplyStep::CreateStored(document_id);
                    Ok(Next::Call(Self::registry_call(document_id)))
                }
                Err(error) => Err(apply_failure(error)),
            },
            ApplyStep::CreateStored(document_id) => {
                let stored = stored_record(result)?;
                let call = self.update_call(document_id, stored);
                self.row()?.meta_resource_id = document_id;
                self.step = ApplyStep::CreateUpdate;
                Ok(Next::Call(call))
            }
            ApplyStep::CreateUpdate => {
                routed_result(result)?.map_err(apply_failure)?;
                let document_id = self.row()?.meta_resource_id;
                self.finish_live(document_id, RecordApplied::Minted)
            }
            ApplyStep::UpdateAccess(document_id) => {
                self.access_result(result)?;
                self.step = ApplyStep::UpdateStored(document_id);
                Ok(Next::Call(Self::registry_call(document_id)))
            }
            ApplyStep::UpdateStored(document_id) => match stored_record(result)? {
                Some(stored) => {
                    let call = self.update_call(document_id, Some(stored));
                    self.step = ApplyStep::Update(document_id, true);
                    Ok(Next::Call(call))
                }
                // Local absence may be registry lag: ask the routed holders to update first and
                // retire the identity only when all report it gone.
                None => {
                    self.step = ApplyStep::UpdatePending(document_id);
                    Ok(Next::Call(Self::pending_call(document_id)))
                }
            },
            ApplyStep::UpdatePending(document_id) => {
                confirm_absent(result, document_id)?;
                let call = self.update_call(document_id, None);
                self.step = ApplyStep::Update(document_id, false);
                Ok(Next::Call(call))
            }
            ApplyStep::Update(document_id, stored) => match routed_result(result)? {
                Ok(()) => self.finish_live(document_id, RecordApplied::Updated),
                Err(HarvestWriteFailure::NotFound) if !stored => {
                    self.row()?.predecessors.push(document_id);
                    self.mint()
                }
                Err(error) => Err(apply_failure(error)),
            },
            // Prove the harvested document is gone before its provenance may go terminal. An
            // empty local read is not evidence of absence, since the create may still be queued
            // here; only a routed delete that succeeds or an all-absent answer retires it.
            ApplyStep::TombstoneAccess(document_id) => {
                self.access_result(result)?;
                self.step = ApplyStep::TombstoneStored(document_id);
                Ok(Next::Call(Self::registry_call(document_id)))
            }
            ApplyStep::TombstoneStored(document_id) => match stored_record(result)? {
                Some(stored) => {
                    let call = self.delete_call(document_id, Some(stored));
                    self.step = ApplyStep::Delete(document_id);
                    Ok(Next::Call(call))
                }
                None => {
                    self.step = ApplyStep::TombstonePending(document_id);
                    Ok(Next::Call(Self::pending_call(document_id)))
                }
            },
            ApplyStep::TombstonePending(document_id) => {
                confirm_absent(result, document_id)?;
                let call = self.delete_call(document_id, None);
                self.step = ApplyStep::Delete(document_id);
                Ok(Next::Call(call))
            }
            ApplyStep::Delete(document_id) => {
                match routed_result(result)? {
                    // Every holder reporting the document absent is the same evidence as a
                    // delete this run performed.
                    Ok(()) | Err(HarvestWriteFailure::NotFound) => {}
                    Err(error) => return Err(apply_failure(error)),
                }
                let row = self.row()?;
                row.meta_resource_id = document_id;
                row.state = HarvestRecordState::Tombstoned;
                self.write_row(ApplyStep::Final(RecordApplied::Tombstoned))
            }
            ApplyStep::Final(applied) => {
                expect_write(event(result)?)?;
                Ok(Next::Done(applied))
            }
        }
    }
}

fn next_version(existing: Option<&HarvestProvenance>) -> u64 {
    existing
        .map(|provenance| provenance.version + 1)
        .unwrap_or(1)
}

/// Land a source record under its target prefix at a stable, path-safe segment.
/// `b64-` carries the OAI identifier as URL-safe base64 when it fits the budget,
/// `b3-` its BLAKE3 digest; the disjoint domains prevent segment collisions.
fn harvest_document_path(prefix: &str, identifier: &str) -> Result<String, HarvestFailure> {
    let Some(prefix) = normalize_target_prefix(prefix) else {
        return Err(permanent(format!(
            "harvest target prefix leaves no room for an encoded identifier: {prefix}"
        )));
    };
    let budget = HARVEST_PATH_BYTES - prefix.len() - 1;
    let encoded = format!(
        "b64-{}",
        base64::Engine::encode(
            &base64::engine::general_purpose::URL_SAFE_NO_PAD,
            identifier
        )
    );
    let segment = if encoded.len() <= budget {
        encoded
    } else {
        format!("b3-{}", blake3::hash(identifier.as_bytes()).to_hex())
    };
    Ok(format!("{prefix}/{segment}"))
}

fn expect_write(event: Event) -> Result<(), HarvestFailure> {
    match event {
        Event::Storage(StorageEvent::WriteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => {
            Err(retryable(format!("harvest write: {error}")))
        }
        _ => Err(retryable("harvest write: unexpected event")),
    }
}

fn permanent(message: impl Into<String>) -> HarvestFailure {
    HarvestFailure::Job(JobError::permanent(message))
}

fn retryable(message: impl Into<String>) -> HarvestFailure {
    HarvestFailure::Job(JobError::retryable(message))
}

fn read_failure(error: StorageReadError) -> HarvestFailure {
    retryable(format!("harvest storage read: {error}"))
}

fn apply_failure(error: HarvestWriteFailure) -> HarvestFailure {
    match error {
        HarvestWriteFailure::Retryable(message) => retryable(format!("harvest apply: {message}")),
        HarvestWriteFailure::Permanent(message) => {
            permanent(format!("harvest apply failed: {message}"))
        }
        HarvestWriteFailure::Rejected(violations) => permanent(format!(
            "harvest record rejected by validation: {violations:?}"
        )),
        error @ (HarvestWriteFailure::NotFound | HarvestWriteFailure::AlreadyExists) => {
            permanent(format!("harvest apply failed: {error}"))
        }
    }
}

#[cfg(test)]
mod pure_tests {
    use super::*;
    use crate::harvest::target_path::DIGEST_SEGMENT_BYTES;

    fn path(identifier: &str) -> String {
        harvest_document_path("/imported/zenodo/", identifier).unwrap()
    }

    #[test]
    fn path_encodes_identifier() {
        let encoded = path("oai:example.org:123/v2");
        assert!(encoded.starts_with("imported/zenodo/b64-"));
        assert!(!encoded["imported/zenodo/".len()..].contains('/'));
    }

    #[test]
    fn separators_never_collide() {
        // ':' and '/' both collapsed to '-' under the old sanitizer.
        let paths = [
            path("oai:a:b"),
            path("oai/a/b"),
            path("oai-a-b"),
            path("oai:a/b"),
        ];
        for (index, left) in paths.iter().enumerate() {
            for right in &paths[index + 1..] {
                assert_ne!(left, right);
            }
        }
    }

    // a clean digest-shaped identifier stays distinct
    #[test]
    fn digest_shape_distinct() {
        // A raw identifier that looks like digest output lands in the b64 domain.
        let digest = blake3::hash(b"oai:example.org:1").to_hex().to_string();
        assert_ne!(path(&format!("b3-{digest}")), path("oai:example.org:1"));
        assert!(path(&format!("b3-{digest}")).contains("/b64-"));
    }

    // a long identifier falls back to a digest
    #[test]
    fn long_identifier_digests() {
        let long = "oai:example.org:".to_string() + &"x".repeat(600);
        let encoded = path(&long);
        assert!(encoded.starts_with("imported/zenodo/b3-"));
        assert_eq!(encoded, path(&long));
        assert!(encoded.len() <= HARVEST_PATH_BYTES);
    }

    // a unicode identifier round-trips distinctly
    #[test]
    fn unicode_stays_distinct() {
        assert_ne!(path("oai:例:1"), path("oai:例:2"));
        assert!(path("oai:例:1").contains("/b64-"));
    }

    #[test]
    fn oversized_prefix_rejected() {
        assert!(harvest_document_path(&"p".repeat(HARVEST_PATH_BYTES), "x").is_err());
    }

    /// Every prefix source creation accepts must yield a bounded path, and
    /// padding must never move a record to a different one.
    #[test]
    fn accepted_prefixes_yield() {
        let longest = "p".repeat(HARVEST_PATH_BYTES - DIGEST_SEGMENT_BYTES - 1);
        for prefix in ["imported/zenodo", " /imported/zenodo/ ", longest.as_str()] {
            assert!(normalize_target_prefix(prefix).is_some());
            let encoded = harvest_document_path(prefix, "oai:example.org:1").unwrap();
            assert!(encoded.len() <= HARVEST_PATH_BYTES);
        }
        assert_eq!(
            path("oai:example.org:1"),
            harvest_document_path(" /imported/zenodo/ ", "oai:example.org:1").unwrap()
        );
    }

    // the next version increments, or starts at one
    #[test]
    fn next_version_increments() {
        assert_eq!(next_version(None), 1);
        let provenance = HarvestProvenance {
            group_id: Ulid::nil(),
            namespace: "ns".to_string(),
            source_record_id: "r".to_string(),
            meta_resource_id: Ulid::nil(),
            version: 4,
            source_datestamp_ms: 0,
            state: HarvestRecordState::Live,
            predecessors: Vec::new(),
        };
        assert_eq!(next_version(Some(&provenance)), 5);
    }
}

#[cfg(test)]
#[path = "harvest_tests.rs"]
mod operation_tests;
