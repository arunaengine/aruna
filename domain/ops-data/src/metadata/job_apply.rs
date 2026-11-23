//! Runs one materialization job against the graph, and the older-job wait before it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;

/// The queued jobs for earlier events of the document that `job` may have to wait for: none
/// that this batch advanced, and none a final status already covers.
pub(super) fn older_candidates(
    group: &GroupJobs,
    job: &MetadataMaterializationRecord,
    advanced: &BTreeSet<Ulid>,
) -> Vec<MetadataMaterializationRecord> {
    (group.pending.iter())
        .filter(|pending| {
            pending.event_id < job.event_id
                && !advanced.contains(&pending.event_id)
                && !(group.status.as_ref())
                    .is_some_and(|status| status_obsoletes_job(status, pending))
        })
        .cloned()
        .collect()
}

#[derive(Debug, PartialEq)]
pub(super) enum OlderStage {
    Event(MetadataMaterializationRecord),
    Prune,
}

// An unadvanced, live job for an earlier event of the same document must run
// first, so this one waits for the next batch. Orphan jobs met on the way are deleted.
#[derive(Debug, PartialEq)]
pub(super) struct OlderJobOperation {
    pub(super) pending: VecDeque<MetadataMaterializationRecord>,
    pub(super) stage: OlderStage,
}

impl OlderJobOperation {
    pub(super) fn new(pending: Vec<MetadataMaterializationRecord>) -> Self {
        Self {
            pending: pending.into(),
            stage: OlderStage::Prune,
        }
    }

    pub(super) fn check(&mut self) -> MaterializationTurn<bool> {
        let Some(pending) = self.pending.pop_front() else {
            return Ok(Next::Done(false));
        };
        let read = event_read(pending.document_id, pending.event_id);
        self.stage = OlderStage::Event(pending);
        Ok(Next::Call(IoCall::Read(vec![read])))
    }
}

impl Operation for OlderJobOperation {
    type Output = bool;
    type Error = MetadataMaterializationError;

    fn first(&mut self) -> MaterializationTurn<bool> {
        self.check()
    }

    fn next(&mut self, result: IoResult) -> MaterializationTurn<bool> {
        let pending = match std::mem::replace(&mut self.stage, OlderStage::Prune) {
            OlderStage::Event(pending) => pending,
            OlderStage::Prune => {
                pruned(result)?;
                return self.check();
            }
        };
        match logged_event(pending.document_id, pending.event_id, result.value()?) {
            // Merges commute and wait for their own dependencies, so an earlier merge never
            // holds back later work; blocking on it could wait for a dot a later event carries.
            Ok(event) if merges(&event) => self.check(),
            Ok(_) => Ok(Next::Done(true)),
            Err(MetadataMaterializationError::CreateEventMissing { .. }) => {
                warn!(document_id = %pending.document_id, event_id = %pending.event_id, "Deleting orphan metadata materialization job");
                let stale = StaleJob {
                    index_key: materialization_job_key(&pending).to_vec(),
                    seen: Some(pending),
                    drop_sidecar: true,
                };
                Ok(Next::Call(IoCall::child(PruneJobsOperation::new(vec![
                    stale,
                ]))))
            }
            Err(error) => Err(error),
        }
    }
}

#[derive(Debug, PartialEq)]
pub(super) struct MaterializedCreateEvent {
    pub(super) raw_revision: Option<MetadataRawRevision>,
    pub(super) raw_state_write: (String, ByteView, ByteView),
    pub(super) validation_write: Option<(String, ByteView, ByteView)>,
}

#[derive(Debug, PartialEq)]
pub(super) enum JobStage {
    Event,
    Deleted,
    Removed,
    Policy,
    Change,
    Export,
    Assess(String),
    Merged(Box<ProfileValidationStatus>),
    Raw,
    Apply(Box<RawEventPlan>),
    /// Exports the graph a job without a displayed revision materialized, for its mark.
    Render(Box<MaterializedCreateEvent>),
    Mark(Box<MaterializedCreateEvent>),
    Checkpoint(Box<MaterializedCreateEvent>),
    Project(Box<MaterializedCreateEvent>),
}

/// Applies one job's event to the graph store, marks its Git snapshot for the capture drain and
/// projects its IRI and data link rows, all left for the finish transaction to store. An event that
/// fails is rescheduled or parked; a terminal failure finishes with a failed status.
#[derive(Debug, PartialEq)]
pub(super) struct MaterializationJobOperation {
    pub(super) job_key: Vec<u8>,
    pub(super) job: MetadataMaterializationRecord,
    pub(super) status: Option<MaterializationStatusRecord>,
    pub(super) cache: RawStateCache,
    pub(super) node: GitNode,
    pub(super) event: Option<MetadataEventRecord>,
    pub(super) now_ms: u64,
    pub(super) apply_started_ms: u64,
    pub(super) craqle_elapsed: Duration,
    pub(super) capture: Option<CaptureMark>,
    pub(super) stage: JobStage,
}

impl MaterializationJobOperation {
    pub(super) fn new(
        (job_key, job): (Vec<u8>, MetadataMaterializationRecord),
        status: Option<MaterializationStatusRecord>,
        cache: RawStateCache,
        node: GitNode,
    ) -> Self {
        Self {
            job_key,
            job,
            status,
            cache,
            node,
            event: None,
            now_ms: 0,
            apply_started_ms: 0,
            craqle_elapsed: Duration::ZERO,
            capture: None,
            stage: JobStage::Event,
        }
    }

    pub(super) fn event(&self) -> Result<&MetadataEventRecord, MetadataMaterializationError> {
        (self.event.as_ref()).ok_or_else(|| {
            MetadataMaterializationError::UnexpectedEvent("materialization event not read".into())
        })
    }

    pub(super) fn document_job_key(&self) -> Vec<u8> {
        document_job_key(self.job.document_id, self.job.event_id).to_vec()
    }

    pub(super) fn finish(
        &mut self,
        mut processed: ProcessedMaterializationJob,
    ) -> MaterializationTurn<ProcessedMaterializationJob> {
        processed.cache = std::mem::take(&mut self.cache);
        Ok(Next::Done(processed))
    }

    pub(super) fn settled(
        &mut self,
        status: Option<MaterializationStatusRecord>,
    ) -> MaterializationTurn<ProcessedMaterializationJob> {
        let job_key = std::mem::take(&mut self.job_key);
        let completed =
            CompletedMaterializationJob::without_rows(job_key, self.document_job_key(), status);
        let elapsed = self.craqle_elapsed;
        self.finish(ProcessedMaterializationJob::completed(completed, elapsed))
    }

    pub(super) fn read(
        &mut self,
        result: IoResult,
    ) -> MaterializationTurn<ProcessedMaterializationJob> {
        let (document_id, event_id) = (self.job.document_id, self.job.event_id);
        let event = (result.value().map_err(MetadataMaterializationError::from))
            .and_then(|value| logged_event(document_id, event_id, value));
        // A merge commutes, so a later materialized event never makes an earlier one obsolete;
        // skipping it would drop a concurrent edit that arrived late.
        let obsolescence = match job_obsolescence(self.status.as_ref(), &self.job) {
            MaterializationJobObsolescence::Final if event.as_ref().is_ok_and(merges) => {
                MaterializationJobObsolescence::Live
            }
            other => other,
        };
        match obsolescence {
            MaterializationJobObsolescence::Live => {}
            MaterializationJobObsolescence::Final => return self.settled(None),
            MaterializationJobObsolescence::RetryAdvanced => {
                self.stage = JobStage::Removed;
                let index = (
                    MATERIALIZATION_JOB_KEYSPACE.to_string(),
                    ByteView::from(std::mem::take(&mut self.job_key)),
                );
                return Ok(Next::Call(IoCall::Delete(vec![index])));
            }
        }
        let event = match event {
            Ok(event) => event,
            Err(MetadataMaterializationError::CreateEventMissing { .. }) => {
                return self.settled(None);
            }
            Err(error) => return Err(error),
        };
        let deleted = GraphDeletedOperation::new(&event.record.graph_iri);
        self.event = Some(event);
        self.stage = JobStage::Deleted;
        Ok(Next::Call(IoCall::child(deleted)))
    }

    pub(super) fn apply(&mut self) -> MaterializationTurn<ProcessedMaterializationJob> {
        self.apply_started_ms = self.now_ms;
        let event = self.event()?;
        if !merges(event) {
            let prepare = PrepareRawOperation::new(event.clone(), self.cache.clone());
            self.stage = JobStage::Raw;
            return Ok(Next::Call(IoCall::child(prepare)));
        }
        // A batch carries quads only, so the event's visibility is applied here the
        // way a crate replacement used to carry it.
        let effect = MetadataEffect::SetGraphPolicy {
            graph_iri: event.record.graph_iri.clone(),
            policy: event_policy(event),
        };
        self.stage = JobStage::Policy;
        Ok(graph_call(effect))
    }

    /// Merges the origin's batch or joins a checkpoint's graph state, then re-renders and
    /// re-validates the graph. Both are order independent and idempotent by dot, so every
    /// holder converges whatever order events arrive in.
    pub(super) fn applied(
        &mut self,
        stage: JobStage,
        result: IoResult,
    ) -> MaterializationTurn<ProcessedMaterializationJob> {
        let Some(event) = self.event.take() else {
            return Err(MetadataMaterializationError::UnexpectedEvent(
                "materialization event not read".into(),
            ));
        };
        let turn = self.apply_step(&event, stage, result);
        self.event = Some(event);
        turn
    }

    pub(super) fn apply_step(
        &mut self,
        event: &MetadataEventRecord,
        stage: JobStage,
        result: IoResult,
    ) -> MaterializationTurn<ProcessedMaterializationJob> {
        let graph_iri = event.record.graph_iri.clone();
        match stage {
            JobStage::Policy => match graph_event(result)? {
                MetadataEvent::GraphPolicySet { .. } => {
                    self.stage = JobStage::Change;
                    Ok(graph_call(graph_materialization_effect(event, None, false)))
                }
                other => Err(unexpected(other)),
            },
            JobStage::Change => match graph_event(result)? {
                MetadataEvent::BatchMerged { .. } | MetadataEvent::SnapshotInstalled { .. } => {
                    self.stage = JobStage::Export;
                    let export = MetadataEffect::ExportRoCrate { graph_iri };
                    Ok(graph_call(export))
                }
                other => Err(unexpected(other)),
            },
            JobStage::Export => match graph_event(result)? {
                MetadataEvent::RoCrateExportResult { jsonld, .. } => {
                    let record = &event.record;
                    let assess = ProfileAssessOperation::render(
                        self.node.node(),
                        record.document_id,
                        record.group_id,
                        jsonld.clone(),
                    );
                    self.stage = JobStage::Assess(jsonld);
                    Ok(Next::Call(IoCall::child(assess)))
                }
                other => Err(unexpected(other)),
            },
            JobStage::Assess(render) => {
                let assessed = result.child::<MetadataProfilePreview, MetadataError>()?;
                let status = render_verdict(assessed, event.record.document_id);
                let findings = violation_count(&status);
                let event = event.clone();
                let prepare =
                    PrepareMergedOperation::new(event, (render, findings), self.cache.clone());
                self.stage = JobStage::Merged(Box::new(status));
                Ok(Next::Call(IoCall::child(prepare)))
            }
            JobStage::Merged(mut status) => {
                let plan = result.child::<RawEventPlan, RawReadError>()??;
                plan.cache(&mut self.cache);
                status.dataset_revision = event.event_id;
                status.dataset_digest =
                    (plan.revision.as_ref()).and_then(|revision| revision.dataset_digest);
                let validation_write = profile_validation_entry(&status)?;
                self.mark(
                    event,
                    MaterializedCreateEvent {
                        raw_revision: plan.revision,
                        raw_state_write: plan.state_write,
                        validation_write: Some(validation_write),
                    },
                )
            }
            JobStage::Raw => {
                let plan = result.child::<RawEventPlan, RawReadError>()??;
                let effect =
                    graph_materialization_effect(event, plan.revision.as_ref(), plan.rebuild);
                self.stage = JobStage::Apply(Box::new(plan));
                Ok(graph_call(effect))
            }
            JobStage::Apply(plan) => match graph_event(result)? {
                MetadataEvent::CreateCrateResult { .. }
                | MetadataEvent::ApplyRoCrateResult { .. }
                | MetadataEvent::EntityUpsertResult { .. } => {
                    plan.cache(&mut self.cache);
                    self.mark(
                        event,
                        MaterializedCreateEvent {
                            raw_revision: plan.revision,
                            raw_state_write: plan.state_write,
                            validation_write: None,
                        },
                    )
                }
                other => Err(unexpected(other)),
            },
            other => Err(MetadataMaterializationError::UnexpectedEvent(format!(
                "materialization step {other:?} is not an apply step"
            ))),
        }
    }

    pub(super) fn apply_failed(
        &mut self,
        error: MetadataMaterializationError,
    ) -> MaterializationTurn<ProcessedMaterializationJob> {
        self.craqle_elapsed = elapsed(self.apply_started_ms, self.now_ms);
        if !is_terminal_error(&error) {
            return self.defer(&error);
        }
        let event = self.event()?;
        let status = materialization_failure_status(
            &self.job,
            event,
            (error.to_string(), self.job.failures),
            true,
            self.now_ms,
        );
        self.settled(Some(status))
    }

    pub(super) fn defer(
        &mut self,
        error: &MetadataMaterializationError,
    ) -> MaterializationTurn<ProcessedMaterializationJob> {
        let event = self.event()?;
        let finished =
            defer_materialization_job(&self.job_key, &self.job, event, error, self.now_ms);
        let elapsed = self.craqle_elapsed;
        self.finish(ProcessedMaterializationJob::deferred(finished, elapsed))
    }

    // Refreshes keep off the raw view until the Git capture drain snapshots the graph the finish
    // stored. The mark holds the displayed revision, or else the graph exported after the apply.
    // The apply step passes the event, as it holds it outside `self.event` while it runs.
    pub(super) fn mark(
        &mut self,
        event: &MetadataEventRecord,
        materialized: MaterializedCreateEvent,
    ) -> MaterializationTurn<ProcessedMaterializationJob> {
        self.craqle_elapsed = elapsed(self.apply_started_ms, self.now_ms);
        let Some(revision) = materialized.raw_revision.as_ref() else {
            let graph_iri = event.record.graph_iri.clone();
            self.stage = JobStage::Render(Box::new(materialized));
            return Ok(graph_call(MetadataEffect::ExportRoCrate { graph_iri }));
        };
        let graph = (revision.winning_event_id, revision.jsonld.clone());
        self.write_mark(materialized, graph)
    }

    pub(super) fn write_mark(
        &mut self,
        materialized: MaterializedCreateEvent,
        (event_id, jsonld): (Ulid, String),
    ) -> MaterializationTurn<ProcessedMaterializationJob> {
        // The hold keeps refreshes off the raw view; the finish queues the mark itself.
        let hold = (
            MATERIALIZATION_CAPTURE_KEYSPACE.to_string(),
            document_job_key(self.job.document_id, self.job.event_id),
            ByteView::from(Vec::new()),
        );
        self.capture = Some(CaptureMark { event_id, jsonld });
        self.stage = JobStage::Mark(Box::new(materialized));
        Ok(Next::Call(IoCall::Write(vec![hold])))
    }

    pub(super) fn projected(
        &mut self,
        materialized: MaterializedCreateEvent,
        rows: LinkRows,
    ) -> MaterializationTurn<ProcessedMaterializationJob> {
        let job_key = std::mem::take(&mut self.job_key);
        let event = self.event()?;
        let status = materialization_success_status(
            &self.job,
            event,
            materialized.raw_revision.as_ref(),
            self.now_ms,
        );
        let completed = CompletedMaterializationJob {
            job_key,
            document_job_key: Some(self.document_job_key()),
            status: Some(status),
            iri_index_writes: rows.writes,
            link_deletes: rows.deletes,
            raw_state_write: Some(materialized.raw_state_write),
            validation_write: materialized.validation_write,
            sync: Some(CompletedMaterializationSync {
                graph_iri: event.record.graph_iri.clone(),
                peers: event.record.holder_node_ids.clone(),
            }),
            merge: merges(event),
            capture: self.capture.take(),
        };
        let elapsed = self.craqle_elapsed;
        self.finish(ProcessedMaterializationJob::completed(completed, elapsed))
    }
}

impl Operation for MaterializationJobOperation {
    type Output = ProcessedMaterializationJob;
    type Error = MetadataMaterializationError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> MaterializationTurn<ProcessedMaterializationJob> {
        let read = event_read(self.job.document_id, self.job.event_id);
        Ok(Next::Call(IoCall::Read(vec![read])))
    }

    fn next(&mut self, result: IoResult) -> MaterializationTurn<ProcessedMaterializationJob> {
        match std::mem::replace(&mut self.stage, JobStage::Removed) {
            JobStage::Event => self.read(result),
            JobStage::Removed => {
                result.done()?;
                self.finish(ProcessedMaterializationJob::default())
            }
            JobStage::Deleted => {
                let deleted = result.child::<bool, GraphDeletedError>()?;
                if !deleted
                    .map_err(GraphDeletedError::into_error::<MetadataMaterializationError>)?
                {
                    return self.apply();
                }
                let message = "metadata graph was deleted before materialization".to_string();
                let event = self.event()?;
                let status = materialization_failure_status(
                    &self.job,
                    event,
                    (message, self.job.failures),
                    true,
                    self.now_ms,
                );
                self.settled(Some(status))
            }
            JobStage::Render(materialized) => match graph_event(result) {
                Ok(MetadataEvent::RoCrateExportResult { jsonld, .. }) => {
                    let event_id = self.job.event_id;
                    self.write_mark(*materialized, (event_id, jsonld))
                }
                Ok(other) => self.defer(&unexpected(other)),
                Err(error) => self.defer(&error),
            },
            JobStage::Mark(materialized) => {
                if let Err(error) = result.done() {
                    return self.defer(&MetadataMaterializationError::from(error));
                }
                let event = self.event()?;
                let checkpoint = CheckpointOperation::new(self.node.node(), event.record.clone());
                self.stage = JobStage::Checkpoint(materialized);
                Ok(Next::Call(IoCall::child(checkpoint)))
            }
            JobStage::Checkpoint(materialized) => {
                let event = self.event()?;
                let document_id = event.record.document_id;
                // The next materialization of this document checks the window again.
                match result.child::<(), CheckpointError>() {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => {
                        warn!(document_id = %document_id, %error, "Metadata checkpoint failed")
                    }
                    Err(error) => {
                        warn!(document_id = %document_id, %error, "Metadata checkpoint failed")
                    }
                }
                let document = (document_id, event.event_id, event.record.graph_iri.clone());
                self.stage = JobStage::Project(materialized);
                Ok(Next::Call(IoCall::child(ProjectRowsOperation::new(
                    document,
                ))))
            }
            JobStage::Project(materialized) => {
                match result.child::<(LinkRows, usize), MetadataIriError>() {
                    Ok(Ok((rows, _))) => self.projected(*materialized, rows),
                    Ok(Err(error)) => self.defer(&MetadataMaterializationError::from(error)),
                    Err(error) => self.defer(&MetadataMaterializationError::from(error)),
                }
            }
            stage => match self.applied(stage, result) {
                Err(error) => self.apply_failed(error),
                turn => turn,
            },
        }
    }
}

pub(super) fn graph_materialization_effect(
    event: &MetadataEventRecord,
    raw_revision: Option<&MetadataRawRevision>,
    rebuild: bool,
) -> MetadataEffect {
    let policy = event_policy(event);
    let deterministic_actor = Some(deterministic_materialization_actor(event.event_id));
    if rebuild && let Some(raw_revision) = raw_revision {
        return MetadataEffect::ApplyRoCrate {
            request: ApplyRoCrateRequest {
                graph_iri: event.record.graph_iri.clone(),
                jsonld: raw_revision.jsonld.clone(),
                policy,
                durability: MetadataRequestDurability::WalAlreadyDurable,
                deterministic_actor,
            },
        };
    }
    match &event.payload {
        MetadataEventPayload::Scaffold {
            name,
            description,
            date_published,
            license,
        } => MetadataEffect::CreateCrate {
            request: Box::new(MetadataCrateRequest {
                graph_iri: event.record.graph_iri.clone(),
                name: name.clone(),
                description: description.clone(),
                date_published: date_published.clone(),
                license: license.clone(),
                policy,
                durability: MetadataRequestDurability::WalAlreadyDurable,
                deterministic_actor,
            }),
        },
        MetadataEventPayload::RoCrate { jsonld }
        | MetadataEventPayload::ReplaceRoCrate { jsonld } => MetadataEffect::ApplyRoCrate {
            request: ApplyRoCrateRequest {
                graph_iri: event.record.graph_iri.clone(),
                jsonld: jsonld.clone(),
                policy,
                durability: MetadataRequestDurability::WalAlreadyDurable,
                deterministic_actor,
            },
        },
        MetadataEventPayload::UpsertDataEntity { jsonld } => MetadataEffect::UpsertDataEntity {
            request: aruna_core::metadata::UpsertEntityRequest {
                graph_iri: event.record.graph_iri.clone(),
                jsonld: jsonld.clone(),
                durability: MetadataRequestDurability::WalAlreadyDurable,
                deterministic_actor,
            },
        },
        MetadataEventPayload::UpsertContextualEntity { jsonld } => {
            MetadataEffect::UpsertContextualEntity {
                request: aruna_core::metadata::UpsertEntityRequest {
                    graph_iri: event.record.graph_iri.clone(),
                    jsonld: jsonld.clone(),
                    durability: MetadataRequestDurability::WalAlreadyDurable,
                    deterministic_actor,
                },
            }
        }
        MetadataEventPayload::ApplyBatch { batch, .. } => MetadataEffect::MergeBatch {
            graph_iri: event.record.graph_iri.clone(),
            batch: batch.clone(),
        },
        // A checkpoint without graph state joins an empty snapshot, which changes nothing.
        MetadataEventPayload::Checkpoint { snapshot } => MetadataEffect::InstallSnapshot {
            graph_iri: event.record.graph_iri.clone(),
            snapshot: snapshot.clone().unwrap_or_else(|| {
                Box::new(craqle::GraphReplicaSnapshot {
                    graph: craqle::GraphId::new(&event.record.graph_iri),
                    clock: craqle::VectorClock::default(),
                    quads: Vec::new(),
                })
            }),
        },
    }
}

pub(super) fn materialization_success_status(
    job: &MetadataMaterializationRecord,
    event: &MetadataEventRecord,
    raw_revision: Option<&MetadataRawRevision>,
    now_ms: u64,
) -> MaterializationStatusRecord {
    MaterializationStatusRecord {
        document_id: event.record.document_id,
        event_id: event.event_id,
        graph_iri: event.record.graph_iri.clone(),
        context_digest: raw_revision.map(|revision| revision.context_digest),
        dataset_digest: raw_revision.and_then(|revision| revision.dataset_digest),
        state: MaterializationState::Materialized,
        attempts: job.attempts.saturating_add(1),
        failures: job.failures,
        last_error: None,
        updated_at_ms: now_ms,
    }
}

pub(super) fn materialization_failure_status(
    job: &MetadataMaterializationRecord,
    event: &MetadataEventRecord,
    (error, failures): (String, u32),
    terminal: bool,
    now_ms: u64,
) -> MaterializationStatusRecord {
    MaterializationStatusRecord {
        document_id: event.record.document_id,
        event_id: event.event_id,
        graph_iri: event.record.graph_iri.clone(),
        context_digest: None,
        dataset_digest: None,
        state: if terminal {
            MaterializationState::Failed
        } else {
            MaterializationState::Pending
        },
        attempts: job.attempts.saturating_add(1),
        failures,
        last_error: Some(error),
        updated_at_ms: now_ms,
    }
}

/// How a failed materialization attempt is charged. Only [`Application`]
/// failures spend the budget that eventually parks a job.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum MaterializationFailureKind {
    Terminal,
    Transient,
    Application,
}

pub(super) fn materialization_failure_kind(
    error: &MetadataMaterializationError,
) -> MaterializationFailureKind {
    match error {
        MetadataMaterializationError::Metadata(
            MetadataError::InvalidInput(_) | MetadataError::Validation(_),
        ) => MaterializationFailureKind::Terminal,
        // A storage failure or an off-contract adapter event is never the
        // document's fault, so it must not spend the budget that parks a job.
        MetadataMaterializationError::Storage(_)
        | MetadataMaterializationError::UnexpectedEvent(_)
        | MetadataMaterializationError::Metadata(
            MetadataError::ChannelClosed
            | MetadataError::MissingDependencies(_)
            | MetadataError::TaskJoin(_)
            | MetadataError::HandleMissing
            | MetadataError::Persist(_)
            | MetadataError::Storage(_),
        ) => MaterializationFailureKind::Transient,
        _ => MaterializationFailureKind::Application,
    }
}

pub(super) fn is_terminal_error(error: &MetadataMaterializationError) -> bool {
    matches!(
        materialization_failure_kind(error),
        MaterializationFailureKind::Terminal
    )
}
