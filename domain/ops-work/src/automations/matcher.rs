//! Durable pending-row journal matcher turning node events into automation firings or kept rows.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;

use aruna_core::applet::record::{
    Applet, Lifecycle as AppletLifecycle, SubmissionReceipt, applet_key, submission_key,
};
use aruna_core::automation::firing::TriggerPayload;
use aruna_core::automation::glob::glob_match;
use aruna_core::automation::keys::{kept_key, seen_key};
use aruna_core::automation::record::{
    Automation, KeptEvent, Lifecycle, PauseReason, RunStatus, Trigger,
};
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::id_mint::IdMint;
use aruna_core::keyspaces::{
    APPLETS_KEYSPACE, AUTOMATIONS_KEYSPACE, EVENT_JOURNAL_KEYSPACE, WORKFLOW_RUN_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::plugin_runs::{AUTO_PENDING_ROW, SourceEvent, SourceKind};
use aruna_core::structs::execution::workflow_run::{RunState, WorkflowRunRecord, run_key};
use aruna_core::structs::identity::features::{Feature, resolve};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::types::{GroupId, Key};
use aruna_ops_data::s3::object::head::{
    HeadObjectError, HeadObjectInput, HeadObjectOperation, HeadObjectResult,
};
use aruna_ops_identity::document_repository::{GetConfigError, GetConfigOperation};
use byteview::ByteView;
use ulid::Ulid;

use super::change::{AutoChange, WriteAutoOperation};
use super::copy_stage::source_length;
use super::start::{AdmitFiringOp, AdmitOutcome, parse_upload};
use super::{AutoError, autos_page};
use crate::plugins::journal::{JournalRow, journal_page};
use crate::plugins::rows::StoreError;

const MATCH_BATCH: usize = 128;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MatchReport {
    pub scanned: usize,
    pub fired: usize,
    pub skipped: usize,
    pub kept: usize,
    pub paused: usize,
}

#[derive(Debug, PartialEq)]
struct PendingMatch {
    automation_index: usize,
    event: SourceEvent,
}

#[derive(Debug, PartialEq)]
enum MatchStep {
    Config,
    Autos,
    Journal,
    Seen {
        automation_index: usize,
        event: Box<SourceEvent>,
    },
    EnrichUpload {
        automation_index: usize,
        event: Box<SourceEvent>,
        bucket: String,
        version_id: Ulid,
        key: String,
    },
    Enrich {
        automation_index: usize,
        event: Box<SourceEvent>,
    },
    PauseChain {
        automation_index: usize,
        event: Box<SourceEvent>,
        payload: Box<TriggerPayload>,
    },
    Admit {
        automation_index: usize,
        event: Box<SourceEvent>,
        payload: Box<TriggerPayload>,
    },
    Write,
    DropPending,
}

/// Scans `EVENT_JOURNAL_KEYSPACE` `AUTO_PENDING_ROW` rows and fires or keeps matched events while
/// enforcing self-loop guards, node/group feature switches, conditions, chain depth and limits.
#[derive(Debug, PartialEq)]
pub struct MatchEventsOperation {
    realm_id: RealmId,
    disabled_features: BTreeSet<Feature>,
    ids: IdMint,
    now_ms: u64,
    config: Option<RealmConfigDocument>,
    autos: Vec<Automation>,
    rows: Vec<JournalRow>,
    current_key: Option<Vec<u8>>,
    matches: Vec<PendingMatch>,
    report: MatchReport,
    step: MatchStep,
}

impl MatchEventsOperation {
    pub fn new(realm_id: RealmId) -> Self {
        Self {
            realm_id,
            disabled_features: BTreeSet::new(),
            ids: IdMint::default(),
            now_ms: 0,
            config: None,
            autos: Vec::new(),
            rows: Vec::new(),
            current_key: None,
            matches: Vec::new(),
            report: MatchReport::default(),
            step: MatchStep::Config,
        }
    }

    pub fn with_disabled_features(mut self, disabled: BTreeSet<Feature>) -> Self {
        self.disabled_features = disabled;
        self
    }

    fn feature_on(&self, feature: Feature, group_id: GroupId) -> bool {
        let Some(config) = &self.config else {
            return false;
        };
        resolve(config, &self.disabled_features, feature, Some(&group_id)).on
    }

    fn coarse_matches(auto: &Automation, event: &SourceEvent) -> bool {
        if auto.group_id != event.group_id || auto.lifecycle == Lifecycle::Removed {
            return false;
        }
        if auto.account.is_some() && event.actor == auto.account {
            return false;
        }
        match event.kind {
            SourceKind::DataUploaded => {
                let Some((bucket, _, key)) = parse_upload(event) else {
                    return false;
                };
                let file = key.rsplit('/').next().unwrap_or(&key);
                auto.spec.triggers.iter().any(|trigger| match trigger {
                    Trigger::DataUploaded {
                        bucket: trigger_bucket,
                        prefix,
                        pattern,
                    } => {
                        bucket == *trigger_bucket
                            && key.starts_with(prefix)
                            && pattern.as_deref().is_none_or(|glob| glob_match(glob, file))
                    }
                    _ => false,
                })
            }
            SourceKind::AppletSubmission => {
                let Ok(applet_id) = Ulid::from_string(&event.resource) else {
                    return false;
                };
                auto.spec.triggers.iter().any(|trigger| match trigger {
                    Trigger::AppletSubmission {
                        applet_id: Some(target_id),
                    } => *target_id == applet_id,
                    Trigger::AppletSubmission { applet_id: None } => true,
                    _ => false,
                })
            }
            SourceKind::RunFinished => {
                let Ok(workflow_id) = Ulid::from_string(&event.resource) else {
                    return false;
                };
                auto.spec.triggers.iter().any(|trigger| {
                    matches!(
                        trigger,
                        Trigger::RunFinished {
                            workflow_id: target_workflow,
                            ..
                        } if *target_workflow == workflow_id
                    )
                })
            }
            _ => false,
        }
    }

    fn next_match(&mut self) -> Result<Next<MatchReport>, AutoError> {
        if let Some(pending) = self.matches.pop() {
            let key = seen_key(
                self.autos[pending.automation_index].automation_id,
                &pending.event.source,
            );
            self.step = MatchStep::Seen {
                automation_index: pending.automation_index,
                event: Box::new(pending.event),
            };
            return Ok(Next::Call(IoCall::Read(vec![(
                AUTOMATIONS_KEYSPACE.to_string(),
                key,
            )])));
        }
        if let Some(row_key) = self.current_key.take() {
            self.step = MatchStep::DropPending;
            return Ok(Next::Call(IoCall::Delete(vec![(
                EVENT_JOURNAL_KEYSPACE.to_string(),
                Key::from(row_key),
            )])));
        }
        self.next_row()
    }

    fn next_row(&mut self) -> Result<Next<MatchReport>, AutoError> {
        let Some((row_key, event)) = self.rows.pop() else {
            return Ok(Next::Done(self.report));
        };
        self.current_key = Some(row_key);
        for automation_index in 0..self.autos.len() {
            if Self::coarse_matches(&self.autos[automation_index], &event) {
                self.matches.push(PendingMatch {
                    automation_index,
                    event: event.clone(),
                });
            }
        }
        self.matches.reverse();
        self.next_match()
    }

    fn write_kept(
        &mut self,
        automation_index: usize,
        event: SourceEvent,
        payload: TriggerPayload,
        reason: &str,
    ) -> Result<Next<MatchReport>, AutoError> {
        let auto_id = self.autos[automation_index].automation_id;
        let event_id = self.ids.mint(self.now_ms);
        let seen = seen_key(auto_id, &event.source);
        let kept = KeptEvent {
            event_id,
            automation_id: auto_id,
            event,
            reason: reason.to_string(),
            kept_at_ms: self.now_ms,
            payload,
        };
        let rows = vec![
            (
                AUTOMATIONS_KEYSPACE.to_string(),
                kept_key(auto_id, event_id),
                ByteView::from(kept.to_bytes()?),
            ),
            (
                AUTOMATIONS_KEYSPACE.to_string(),
                seen,
                ByteView::from(self.now_ms.to_be_bytes().to_vec()),
            ),
        ];
        self.report.kept += 1;
        self.step = MatchStep::Write;
        Ok(Next::Call(IoCall::Write(rows)))
    }

    fn pause_chain_depth(
        &mut self,
        automation_index: usize,
        event: SourceEvent,
        payload: TriggerPayload,
    ) -> Result<Next<MatchReport>, AutoError> {
        let auto = &self.autos[automation_index];
        if auto.lifecycle == Lifecycle::Active {
            let actor = auto.approved_by.unwrap_or(auto.created_by);
            let pause = WriteAutoOperation::node(
                auto.automation_id,
                AutoChange::Pause(PauseReason::ChainDepth),
                actor,
                self.realm_id,
            );
            self.step = MatchStep::PauseChain {
                automation_index,
                event: Box::new(event),
                payload: Box::new(payload),
            };
            return Ok(Next::Call(IoCall::retry(
                pause,
                &auto.automation_id.to_bytes(),
            )));
        }
        self.write_kept(automation_index, event, payload, "chain_depth")
    }

    fn decide(
        &mut self,
        automation_index: usize,
        event: SourceEvent,
        payload: TriggerPayload,
    ) -> Result<Next<MatchReport>, AutoError> {
        let auto = &self.autos[automation_index];
        if auto.lifecycle != Lifecycle::Active {
            return self.write_kept(automation_index, event, payload, auto.lifecycle.as_str());
        }
        if !self.feature_on(Feature::Automations, auto.group_id) {
            return self.write_kept(automation_index, event, payload, "feature_disabled");
        }
        if matches!(payload, TriggerPayload::AppletSubmission { .. })
            && !self.feature_on(Feature::Applets, auto.group_id)
        {
            return self.write_kept(automation_index, event, payload, "applets_disabled");
        }
        let auto_id = auto.automation_id;
        let firing_id = self.ids.mint(self.now_ms);
        let admit = AdmitFiringOp::journal(
            auto_id,
            firing_id,
            event.source.clone(),
            event.actor,
            payload.clone(),
        );
        self.step = MatchStep::Admit {
            automation_index,
            event: Box::new(event),
            payload: Box::new(payload),
        };
        Ok(Next::Call(IoCall::retry(admit, &auto_id.to_bytes())))
    }
}

impl Operation for MatchEventsOperation {
    type Output = MatchReport;
    type Error = AutoError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn seed(&mut self, seed: u128) {
        self.ids = IdMint::new(seed);
    }

    fn first(&mut self) -> Result<Next<MatchReport>, AutoError> {
        self.step = MatchStep::Config;
        Ok(Next::Call(IoCall::child(GetConfigOperation::new(
            self.realm_id,
        ))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<MatchReport>, AutoError> {
        match std::mem::replace(&mut self.step, MatchStep::Write) {
            MatchStep::Config => {
                self.config = result.child::<RealmConfigDocument, GetConfigError>()?.ok();
                self.step = MatchStep::Autos;
                Ok(Next::Call(autos_page(None)))
            }
            MatchStep::Autos => {
                let (rows, next) = result.page()?;
                for (_, value) in rows {
                    let auto = Automation::from_bytes(&value)?;
                    if auto.lifecycle != Lifecycle::Removed {
                        self.autos.push(auto);
                    }
                }
                if let Some(after) = next {
                    self.step = MatchStep::Autos;
                    return Ok(Next::Call(autos_page(Some(after))));
                }
                self.step = MatchStep::Journal;
                Ok(Next::Call(IoCall::child(journal_page(
                    AUTO_PENDING_ROW,
                    None,
                    MATCH_BATCH,
                ))))
            }
            MatchStep::Journal => {
                let (mut rows, _) = result
                    .child::<(Vec<JournalRow>, Option<Vec<u8>>), StoreError>()?
                    .map_err(|error| AutoError::Storage(error.detail()))?;
                self.report.scanned = rows.len();
                rows.reverse();
                self.rows = rows;
                self.next_row()
            }
            MatchStep::Seen {
                automation_index,
                event,
            } => {
                if result.value()?.is_some() {
                    return self.next_match();
                }
                match event.kind {
                    SourceKind::DataUploaded => {
                        let Some((bucket, version_id, key)) = parse_upload(&event) else {
                            return self.next_match();
                        };
                        let head = HeadObjectOperation::new(HeadObjectInput {
                            bucket: bucket.clone(),
                            key: key.clone(),
                            version_id: Some(version_id),
                        });
                        self.step = MatchStep::EnrichUpload {
                            automation_index,
                            event,
                            bucket,
                            version_id,
                            key,
                        };
                        Ok(Next::Call(IoCall::child(head)))
                    }
                    SourceKind::AppletSubmission => {
                        let (Ok(applet_id), Ok(version_id)) = (
                            Ulid::from_string(&event.resource),
                            Ulid::from_string(&event.revision),
                        ) else {
                            return self.next_match();
                        };
                        self.step = MatchStep::Enrich {
                            automation_index,
                            event,
                        };
                        Ok(Next::Call(IoCall::Read(vec![
                            (
                                APPLETS_KEYSPACE.to_string(),
                                submission_key(applet_id, version_id),
                            ),
                            (APPLETS_KEYSPACE.to_string(), applet_key(applet_id)),
                        ])))
                    }
                    SourceKind::RunFinished => {
                        let Ok(run_id) = Ulid::from_string(&event.revision) else {
                            return self.next_match();
                        };
                        self.step = MatchStep::Enrich {
                            automation_index,
                            event,
                        };
                        Ok(Next::Call(IoCall::Read(vec![(
                            WORKFLOW_RUN_KEYSPACE.to_string(),
                            run_key(run_id),
                        )])))
                    }
                    _ => self.next_match(),
                }
            }
            MatchStep::EnrichUpload {
                automation_index,
                event,
                bucket,
                version_id,
                key,
            } => {
                let size_bytes = match result.child::<HeadObjectResult, HeadObjectError>()? {
                    Ok(head) => source_length(&head),
                    Err(
                        HeadObjectError::NoSuchKey
                        | HeadObjectError::NoSuchVersion
                        | HeadObjectError::DeleteMarker,
                    ) => None,
                    Err(error) => return Err(AutoError::Storage(error.to_string())),
                };
                let payload = TriggerPayload::DataUploaded {
                    bucket,
                    key,
                    version_id,
                    path: event.path.clone(),
                    size_bytes,
                };
                self.decide(automation_index, *event, payload)
            }
            MatchStep::Enrich {
                automation_index,
                event,
            } => match event.kind {
                SourceKind::AppletSubmission => {
                    let values = result.values()?;
                    let Some(bytes) = values.first().and_then(Option::as_ref) else {
                        return self.next_match();
                    };
                    let receipt = SubmissionReceipt::from_bytes(bytes)?;
                    let auto = &self.autos[automation_index];
                    let explicit = auto.spec.triggers.iter().any(|trigger| {
                        matches!(
                            trigger,
                            Trigger::AppletSubmission { applet_id: Some(target_id) }
                                if *target_id == receipt.applet_id
                        )
                    });
                    if !explicit {
                        let linked = values
                            .get(1)
                            .and_then(Option::as_deref)
                            .and_then(|raw| Applet::from_bytes(raw).ok())
                            .is_some_and(|applet| {
                                applet.lifecycle != AppletLifecycle::TakenDown
                                    && applet.group_id == auto.group_id
                                    && applet.automations.contains(&auto.automation_id)
                            });
                        if !linked {
                            return self.next_match();
                        }
                    }
                    let payload = TriggerPayload::AppletSubmission {
                        applet_id: receipt.applet_id,
                        version_id: receipt.version_id,
                        pseudonym: receipt.pseudonym,
                        guest: receipt.guest,
                        outside_input: receipt.outside_input || receipt.guest,
                        bucket: receipt.bucket,
                        key: receipt.key,
                        files: receipt.files,
                    };
                    self.decide(automation_index, *event, payload)
                }
                SourceKind::RunFinished => {
                    let Some(bytes) = result.value()? else {
                        return self.next_match();
                    };
                    let run = WorkflowRunRecord::from_bytes(&bytes)?;
                    let auto = &self.autos[automation_index];
                    if run
                        .automation
                        .as_ref()
                        .is_some_and(|origin| origin.automation_id == auto.automation_id)
                    {
                        return self.next_match();
                    }
                    let status = match run.state {
                        RunState::Succeeded => RunStatus::Succeeded,
                        RunState::Failed => RunStatus::Failed,
                        _ => return self.next_match(),
                    };
                    let run_applet = run
                        .automation
                        .as_ref()
                        .and_then(|origin| origin.applet_id)
                        .or_else(|| run.applet.as_ref().map(|origin| origin.applet_id));
                    let matched = auto.spec.triggers.iter().any(|trigger| match trigger {
                        Trigger::RunFinished {
                            workflow_id,
                            status: want_status,
                            applet_id: want_applet,
                        } => {
                            *workflow_id == run.workflow_id
                                && want_status.is_none_or(|wanted| wanted == status)
                                && want_applet.is_none_or(|wanted| Some(wanted) == run_applet)
                        }
                        _ => false,
                    });
                    if !matched {
                        return self.next_match();
                    }
                    let depth = run
                        .automation
                        .as_ref()
                        .map_or(0, |origin| origin.depth)
                        .saturating_add(1);
                    let outside_input = run
                        .automation
                        .as_ref()
                        .is_some_and(|origin| origin.outside_input)
                        || run
                            .applet
                            .as_ref()
                            .is_some_and(|origin| origin.outside_input);
                    let payload = TriggerPayload::RunFinished {
                        workflow_id: run.workflow_id,
                        run_id: run.run_id,
                        status,
                        outputs: run.outputs,
                        applet_id: run_applet,
                        outside_input,
                        depth,
                    };
                    self.decide(automation_index, *event, payload)
                }
                _ => self.next_match(),
            },
            MatchStep::PauseChain {
                automation_index,
                event,
                payload,
            } => {
                if let Ok(updated) = result.child::<Automation, AutoError>()? {
                    self.autos[automation_index] = updated;
                    self.report.paused += 1;
                }
                self.write_kept(automation_index, *event, *payload, "chain_depth")
            }
            MatchStep::Admit {
                automation_index,
                event,
                payload,
            } => match result.child::<AdmitOutcome, AutoError>()? {
                Ok(AdmitOutcome::Created(_)) => {
                    self.report.fired += 1;
                    self.next_match()
                }
                Ok(AdmitOutcome::Skipped(_)) => {
                    self.report.skipped += 1;
                    self.next_match()
                }
                Ok(AdmitOutcome::ChainDepth) => {
                    self.pause_chain_depth(automation_index, *event, *payload)
                }
                Ok(
                    AdmitOutcome::AlreadySeen | AdmitOutcome::Existing(_) | AdmitOutcome::Unmatched,
                ) => self.next_match(),
                Err(AutoError::Limited(message)) => {
                    let reason = if message.contains("active") {
                        "max_active_firings"
                    } else {
                        "rate_limited"
                    };
                    self.write_kept(automation_index, *event, *payload, reason)
                }
                Err(AutoError::Conflict(_)) => {
                    self.write_kept(automation_index, *event, *payload, "paused")
                }
                Err(AutoError::NotFound) => self.next_match(),
                Err(error) => Err(error),
            },
            MatchStep::Write => {
                result.done()?;
                self.next_match()
            }
            MatchStep::DropPending => {
                result.done()?;
                self.next_row()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::calls::ChildOutcome;
    use aruna_core::structs::identity::features::FeatureMode;
    use aruna_core::structs::storage::blob::group_permission_path;
    use aruna_core::user::id::UserId;

    fn sample_auto(realm: RealmId, node: aruna_core::id::NodeId, group: GroupId) -> Automation {
        let owner = UserId::local(Ulid::from_bytes([1; 16]), realm);
        Automation {
            automation_id: Ulid::from_bytes([22; 16]),
            group_id: group,
            name: "chain".into(),
            description: String::new(),
            host_node: node,
            lifecycle: Lifecycle::Active,
            pause_reason: None,
            spec: aruna_core::automation::record::Spec::default(),
            pending: None,
            account: None,
            role_id: None,
            approved_by: Some(owner),
            approved_at_ms: Some(1),
            created_by: owner,
            created_at_ms: 1,
            updated_at_ms: 1,
            revision: 1,
            identity_synced_revision: 1,
            workflow_id: None,
        }
    }

    #[test]
    fn admit_depth_pauses() {
        let realm = RealmId([9; 32]);
        let node = iroh::SecretKey::from_bytes(&[7; 32]).public();
        let group = Ulid::from_bytes([11; 16]);
        let owner = UserId::local(Ulid::from_bytes([1; 16]), realm);
        let auto = sample_auto(realm, node, group);
        let path = group_permission_path(realm, group, node);
        let event = SourceEvent::run_finished(
            (group, path),
            (Ulid::from_bytes([60; 16]), Ulid::from_bytes([61; 16])),
            "succeeded",
            owner,
            10,
        );
        let payload = TriggerPayload::RunFinished {
            workflow_id: Ulid::from_bytes([60; 16]),
            run_id: Ulid::from_bytes([61; 16]),
            status: RunStatus::Succeeded,
            outputs: vec![],
            applet_id: None,
            outside_input: false,
            depth: 3,
        };
        let mut operation = MatchEventsOperation::new(realm);
        operation.autos.push(auto.clone());
        operation.step = MatchStep::Admit {
            automation_index: 0,
            event: Box::new(event),
            payload: Box::new(payload),
        };
        let next = operation
            .next(IoResult::Child(ChildOutcome::new(Ok::<_, AutoError>(
                AdmitOutcome::ChainDepth,
            ))))
            .unwrap();
        assert!(matches!(next, Next::Call(IoCall::Child(_))));
        assert!(matches!(
            operation.step,
            MatchStep::PauseChain {
                automation_index: 0,
                ..
            }
        ));
        let mut paused = auto;
        paused.lifecycle = Lifecycle::Paused;
        paused.pause_reason = Some(PauseReason::ChainDepth);
        let write = operation
            .next(IoResult::Child(ChildOutcome::new(Ok::<_, AutoError>(
                paused,
            ))))
            .unwrap();
        assert_eq!(operation.report.paused, 1);
        assert_eq!(operation.report.kept, 1);
        assert!(matches!(write, Next::Call(IoCall::Write(rows)) if rows.len() == 2));
    }

    #[test]
    fn handles_upload_head() {
        let realm = RealmId([9; 32]);
        let node = iroh::SecretKey::from_bytes(&[7; 32]).public();
        let group = Ulid::from_bytes([11; 16]);
        let owner = UserId::local(Ulid::from_bytes([1; 16]), realm);
        let auto = sample_auto(realm, node, group);
        let version_id = Ulid::from_bytes([30; 16]);
        let event = SourceEvent::data_uploaded(
            (realm, group, node),
            ("raw", "in/file.csv"),
            version_id,
            Some(owner),
            10,
        );
        let mut config = RealmConfigDocument::default_for_realm(realm, Vec::new());
        config.features.insert(
            Feature::Automations,
            FeatureMode::Scoped {
                default: true,
                groups: Default::default(),
            },
        );

        // StorageError propagates so the pending journal row is retried rather than skipped.
        let mut retry_op = MatchEventsOperation::new(realm);
        retry_op.config = Some(config.clone());
        retry_op.autos.push(auto.clone());
        retry_op.step = MatchStep::EnrichUpload {
            automation_index: 0,
            event: Box::new(event.clone()),
            bucket: "raw".into(),
            version_id,
            key: "in/file.csv".into(),
        };
        let storage_err = retry_op.next(IoResult::Child(ChildOutcome::new(Err::<
            HeadObjectResult,
            HeadObjectError,
        >(
            HeadObjectError::StorageError(aruna_core::errors::StorageError::CommitFailed),
        ))));
        assert!(matches!(storage_err, Err(AutoError::Storage(_))));

        // NoSuchKey yields size_bytes = None and proceeds to Admit.
        let mut missing_op = MatchEventsOperation::new(realm);
        missing_op.config = Some(config);
        missing_op.autos.push(auto);
        missing_op.step = MatchStep::EnrichUpload {
            automation_index: 0,
            event: Box::new(event),
            bucket: "raw".into(),
            version_id,
            key: "in/file.csv".into(),
        };
        let next = missing_op
            .next(IoResult::Child(ChildOutcome::new(Err::<
                HeadObjectResult,
                HeadObjectError,
            >(
                HeadObjectError::NoSuchKey,
            ))))
            .unwrap();
        assert!(matches!(next, Next::Call(IoCall::Child(_))));
        assert!(matches!(
            missing_op.step,
            MatchStep::Admit { ref payload, .. }
                if matches!(payload.as_ref(), TriggerPayload::DataUploaded { size_bytes: None, .. })
        ));
    }
}
