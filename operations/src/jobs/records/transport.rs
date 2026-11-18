//! Serves job record and launch offer messages from other realm nodes.
//! A relaying peer is authenticated on its own and is never treated as the record author.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, FetchCursor, JobRecordFrame, LaunchFrame, NetEffect, PageLimit};
use aruna_core::events::{JobRecordPage, JobRecordRejection, LaunchDecline};
use aruna_core::operation::Operation;
use aruna_core::placement::holds_placement;
use aruna_core::structs::execution::job::record_rows::PendingNeed;
use aruna_core::structs::execution::job::{
    JobFamilyId, JobFamilyRecord, JobRecordEnvelope, JobRecordError, JobRecordKind,
    PhysicalExecutionState, SubmissionId,
};
use aruna_core::structs::identity::group_delete::GroupWriteError;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::placement::record::PlacementRef;
use tracing::warn;

use super::RecordStoreError;
use super::admit::Admission;
use super::append::{AppendOutcome, AppendRecordConfig, AppendRecordOperation, RecordOrigin};
use super::audit::{AuditPage, AuditScope, FamilyAuditConfig, FamilyAuditOperation};
use super::family::{FamilyReadError, LoadFamilyOperation};
use super::pages::{KeepPagesOperation, PageReadError, ServingNode};
use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::jobs::lifecycle::LifecycleError;
use crate::jobs::lifecycle::cancel::CancelLocalRunsOperation;
use crate::jobs::lifecycle::launch_admit::{
    AdmitLaunchOperation, LaunchDecision, LaunchNode, LaunchUndecided,
};
use crate::jobs::lifecycle::witness_arm::ArmFamilyOperation;
use crate::metadata::protocol::{JobPageReply, MetadataTransportMessage};
use crate::metadata::transport_message_kind;

#[derive(Debug, PartialEq)]
struct PageRequest {
    submission_id: SubmissionId,
    request_digest: Option<[u8; 32]>,
    cursor: Option<FetchCursor>,
    limit: PageLimit,
}

/// What a peer asks of this holder.
#[derive(Debug, PartialEq)]
enum RecordRequest {
    Publish(Box<JobRecordFrame>),
    Page(PageRequest),
}

/// A refusal this node stands behind, or a local failure that says nothing
/// about the record. A transient local failure must never be answered as a
/// definitive refusal: the publisher would stop retrying a valid record.
#[derive(Debug, PartialEq)]
enum ServeError {
    Refused(JobRecordRejection),
    Unavailable,
}

#[derive(Debug, PartialEq)]
enum ServeStep {
    Config,
    Append,
    Dashboard,
    Armed,
    Pages,
    Cancels,
    Stopped,
    Audit,
}

/// What a stored record still requires of this node.
#[derive(Debug, PartialEq)]
struct Accepted {
    family: JobFamilyId,
    admission: Admission,
    rearms: bool,
    paged: Option<JobFamilyRecord>,
}

/// Serves one inbound record publish or page request from an authenticated peer. This node
/// must hold the family's placement, and the peer must be a sync-eligible node of this realm.
#[derive(Debug, PartialEq)]
pub struct ServeRecordOperation {
    peer: NodeId,
    placement: PlacementRef,
    request: Option<RecordRequest>,
    serving: ServingNode,
    now_ms: u64,
    /// Whether the peer asked for a page, which picks the answer's form.
    paging: bool,
    rejected: Option<String>,
    accepted: Option<Box<Accepted>>,
    step: ServeStep,
}

type ServeNext = Result<Next<MetadataTransportMessage>, IoError>;

fn record_answer(result: Result<(), ServeError>) -> MetadataTransportMessage {
    match result {
        Ok(()) => MetadataTransportMessage::ForwardedJobRecord { result: Ok(()) },
        Err(ServeError::Refused(reason)) => MetadataTransportMessage::ForwardedJobRecord {
            result: Err(reason),
        },
        Err(ServeError::Unavailable) => MetadataTransportMessage::ForwardedWriteUnavailable,
    }
}

fn page_answer(result: Result<JobPageReply, ServeError>) -> MetadataTransportMessage {
    match result {
        Ok(reply) => MetadataTransportMessage::ForwardedRecordPage { result: Ok(reply) },
        Err(ServeError::Refused(reason)) => MetadataTransportMessage::ForwardedRecordPage {
            result: Err(reason),
        },
        Err(ServeError::Unavailable) => MetadataTransportMessage::ForwardedWriteUnavailable,
    }
}

/// The page a peer gets from one audit read.
fn audit_reply(audit: AuditPage) -> Result<JobPageReply, ServeError> {
    let frames: Vec<JobRecordFrame> = audit
        .records
        .into_iter()
        .filter_map(|envelope| JobRecordFrame::new(envelope).ok())
        .collect();
    let page = JobRecordPage::new(frames).map_err(|error| {
        warn!(error = %error, "Job record page exceeds its bound");
        ServeError::Unavailable
    })?;
    Ok(JobPageReply {
        page,
        next: audit.next,
    })
}

fn append_failure(error: RecordStoreError) -> ServeError {
    if matches!(
        error,
        RecordStoreError::GroupWrite(GroupWriteError::Deleted)
    ) {
        return ServeError::Refused(JobRecordRejection::Unauthorized);
    }
    warn!(error = %error, "Job record append failed");
    ServeError::Unavailable
}

/// What an admission verdict tells the publisher; `None` for a stored record.
fn admission_refusal(admission: &Admission) -> Option<ServeError> {
    match admission {
        Admission::Authentic | Admission::Duplicate => None,
        Admission::Local
        | Admission::Pending(
            PendingNeed::Evidence(_) | PendingNeed::LocalView | PendingNeed::HolderView,
        )
        | Admission::PendingFull => Some(ServeError::Unavailable),
        Admission::Conflict => Some(ServeError::Refused(JobRecordRejection::Conflict)),
        Admission::Rejected(
            JobRecordError::BadSignature
            | JobRecordError::WrongPublisher(_)
            | JobRecordError::NotHolder(_)
            | JobRecordError::Unauthorized,
        ) => Some(ServeError::Refused(JobRecordRejection::Unauthorized)),
        Admission::Rejected(_) => Some(ServeError::Refused(JobRecordRejection::Invalid)),
    }
}

impl ServeRecordOperation {
    pub fn new(
        peer: NodeId,
        message: MetadataTransportMessage,
        serving: ServingNode,
        now_ms: u64,
    ) -> Self {
        let (placement, request, rejected) = match message {
            MetadataTransportMessage::ForwardJobRecord { placement, record } => {
                (placement, Some(RecordRequest::Publish(record)), None)
            }
            MetadataTransportMessage::ForwardRecordPage {
                placement,
                submission_id,
                request_digest,
                cursor,
                limit,
            } => {
                let page = PageRequest {
                    submission_id,
                    request_digest,
                    cursor,
                    limit,
                };
                (placement, Some(RecordRequest::Page(page)), None)
            }
            other => {
                let kind = transport_message_kind(&other).to_string();
                (PlacementRef::NIL, None, Some(kind))
            }
        };
        Self {
            peer,
            placement,
            paging: matches!(request, Some(RecordRequest::Page(_))),
            request,
            serving,
            now_ms,
            rejected,
            accepted: None,
            step: ServeStep::Config,
        }
    }

    fn fail(&self, error: ServeError) -> ServeNext {
        Ok(Next::Done(match self.paging {
            true => page_answer(Err(error)),
            false => record_answer(Err(error)),
        }))
    }

    fn accepted_ok(&self) -> ServeNext {
        Ok(Next::Done(record_answer(Ok(()))))
    }

    /// Checks this node's authority, and that the family placement it derives itself is the
    /// one the requester named.
    fn authorized(
        &self,
        config: &RealmConfigDocument,
        submission: SubmissionId,
    ) -> Result<(), ServeError> {
        let (local, _) = self.serving.ok_or(ServeError::Unavailable)?;
        let eligible = config
            .sync_eligible_nodes()
            .is_ok_and(|nodes| nodes.contains(&self.peer));
        if !eligible {
            return Err(ServeError::Refused(JobRecordRejection::Unauthorized));
        }
        if !holds_placement(config, &self.placement, local) {
            return Err(ServeError::Refused(JobRecordRejection::NotHolder));
        }
        let derived = config
            .family_placement(submission)
            .map_err(|_| ServeError::Refused(JobRecordRejection::NotHolder))?;
        if derived != self.placement {
            return Err(ServeError::Refused(JobRecordRejection::Invalid));
        }
        Ok(())
    }

    fn configured(&mut self, config: RealmConfigDocument) -> ServeNext {
        let Some(request) = self.request.take() else {
            return Err(IoError::Unfinished);
        };
        match request {
            RecordRequest::Publish(record) => {
                let family = record.envelope().family();
                if let Err(error) = self.authorized(&config, family.submission_id) {
                    return self.fail(error);
                }
                self.append(*record)
            }
            RecordRequest::Page(request) => {
                if let Err(error) = self.authorized(&config, request.submission_id) {
                    return self.fail(error);
                }
                let scope = match request.request_digest {
                    Some(request_digest) => AuditScope::Family(JobFamilyId {
                        submission_id: request.submission_id,
                        request_digest,
                    }),
                    None => AuditScope::Submission(request.submission_id),
                };
                self.step = ServeStep::Audit;
                let audit = FamilyAuditOperation::new(FamilyAuditConfig {
                    scope,
                    cursor: request.cursor,
                    limit: request.limit,
                });
                Ok(Next::Call(IoCall::child(audit)))
            }
        }
    }

    fn append(&mut self, record: JobRecordFrame) -> ServeNext {
        let Some((local, realm_id)) = self.serving else {
            return self.fail(ServeError::Unavailable);
        };
        let envelope = record.envelope();
        self.accepted = Some(Box::new(Accepted {
            family: envelope.family(),
            admission: Admission::Duplicate,
            rearms: rearms_witness(envelope),
            paged: binds_pages(&envelope.record).then(|| envelope.record.clone()),
        }));
        self.step = ServeStep::Append;
        let append = AppendRecordOperation::new(AppendRecordConfig {
            realm_id,
            local_node_id: local,
            record,
            local: None,
            origin: RecordOrigin::Peer(self.peer),
            now_ms: self.now_ms,
        });
        Ok(Next::Call(IoCall::child(append)))
    }

    fn accepted(&mut self) -> Result<&mut Accepted, IoError> {
        self.accepted.as_deref_mut().ok_or(IoError::Unfinished)
    }

    fn appended(&mut self, outcome: AppendOutcome) -> ServeNext {
        let authentic = matches!(outcome.admission, Admission::Authentic);
        self.accepted()?.admission = outcome.admission;
        if !authentic {
            return self.settle();
        }
        self.step = ServeStep::Dashboard;
        let changed = Effect::Net(NetEffect::DashboardChanged);
        Ok(Next::Call(IoCall::Effect(Box::new(changed))))
    }

    fn notified(&mut self) -> ServeNext {
        let (family, rearms) = {
            let accepted = self.accepted()?;
            (accepted.family, accepted.rearms)
        };
        let (true, Some((local, realm_id))) = (rearms, self.serving) else {
            return self.settle();
        };
        self.step = ServeStep::Armed;
        let arm = ArmFamilyOperation::new(family, local, realm_id, self.now_ms);
        Ok(Next::Call(IoCall::child(arm)))
    }

    /// The publisher retries until this holder keeps the pages the record binds.
    fn settle(&mut self) -> ServeNext {
        let serving = self.serving;
        let peer = self.peer;
        let accepted = self.accepted()?;
        if let Some(refusal) = admission_refusal(&accepted.admission) {
            return self.fail(refusal);
        }
        if let Some(record) = accepted.paged.take() {
            self.step = ServeStep::Pages;
            let keep = KeepPagesOperation::new(&record, peer, serving);
            return Ok(Next::Call(IoCall::child(keep)));
        }
        self.read_cancels()
    }

    fn read_cancels(&mut self) -> ServeNext {
        let family = self.accepted()?.family;
        self.step = ServeStep::Cancels;
        let read = LoadFamilyOperation::kind(family, JobRecordKind::Cancel);
        Ok(Next::Call(IoCall::child(read)))
    }

    /// Admission may promote a pending cancel, and retries must reapply its side effect.
    fn cancels_read(&mut self, cancels: Vec<JobRecordEnvelope>) -> ServeNext {
        if cancels.is_empty() {
            return self.accepted_ok();
        }
        let family = self.accepted()?.family;
        self.step = ServeStep::Stopped;
        let cancel = CancelLocalRunsOperation::new(family, self.serving, self.now_ms);
        Ok(Next::Call(IoCall::child(cancel)))
    }
}

impl Operation for ServeRecordOperation {
    type Output = MetadataTransportMessage;
    type Error = IoError;

    fn first(&mut self) -> ServeNext {
        if let Some(kind) = self.rejected.take() {
            return Ok(Next::Done(MetadataTransportMessage::Reject(format!(
                "unexpected job record request {kind}"
            ))));
        }
        let Some((_, realm_id)) = self.serving else {
            return self.fail(ServeError::Unavailable);
        };
        Ok(Next::Call(IoCall::child(GetConfigOperation::new(realm_id))))
    }

    fn next(&mut self, result: IoResult) -> ServeNext {
        match self.step {
            ServeStep::Config => match result.child::<RealmConfigDocument, GetConfigError>()? {
                Ok(config) => self.configured(config),
                Err(_) => self.fail(ServeError::Unavailable),
            },
            ServeStep::Append => match result.child::<AppendOutcome, RecordStoreError>()? {
                Ok(outcome) => self.appended(outcome),
                Err(error) => self.fail(append_failure(error)),
            },
            ServeStep::Dashboard => self.notified(),
            ServeStep::Armed => {
                if let Err(error) = result.child::<(), LifecycleError>()? {
                    warn!(error = %error, "Witness deadline was not armed");
                }
                self.settle()
            }
            ServeStep::Pages => match result.child::<(), PageReadError>()? {
                Ok(()) => self.read_cancels(),
                Err(error) => {
                    warn!(error = %error, "Job record input pages unavailable");
                    self.fail(ServeError::Unavailable)
                }
            },
            ServeStep::Cancels => {
                match result.child::<Vec<JobRecordEnvelope>, FamilyReadError>()? {
                    Ok(cancels) => self.cancels_read(cancels),
                    Err(_) => self.fail(ServeError::Unavailable),
                }
            }
            ServeStep::Stopped => self.accepted_ok(),
            ServeStep::Audit => match result.child::<AuditPage, RecordStoreError>()? {
                Ok(audit) => Ok(Next::Done(page_answer(audit_reply(audit)))),
                Err(error) => {
                    warn!(error = %error, "Job record page read failed");
                    self.fail(ServeError::Unavailable)
                }
            },
        }
    }
}

fn binds_pages(record: &JobFamilyRecord) -> bool {
    match record {
        JobFamilyRecord::Spec(spec) => spec.input_pages.is_some(),
        JobFamilyRecord::Launch(launch) => launch.input_pages.is_some(),
        _ => false,
    }
}

/// Records that must schedule a witness round here: a claim makes this holder a
/// witness, a cancellation stops the round it armed, and an execution that ended
/// in an infrastructure error leaves the family to be planned again.
fn rearms_witness(envelope: &JobRecordEnvelope) -> bool {
    match &envelope.record {
        JobFamilyRecord::Claim(_) | JobFamilyRecord::Cancel(_) => true,
        JobFamilyRecord::Update(update) => update.state == PhysicalExecutionState::Error,
        _ => false,
    }
}

/// Serves one inbound launch offer. The offer is bounded and kind-checked at decode; admission,
/// reservation and receipt are the target's own decision. An undecidable offer is answered as
/// unavailable so the scheduler can retry. `node` is `None` on a node without a network.
#[derive(Debug, PartialEq)]
pub struct ServeLaunchOperation {
    peer: NodeId,
    offer: Option<(LaunchFrame, LaunchNode)>,
    admitting: bool,
}

type LaunchNext = Result<Next<MetadataTransportMessage>, IoError>;

fn launch_unavailable() -> LaunchNext {
    Ok(Next::Done(
        MetadataTransportMessage::ForwardedWriteUnavailable,
    ))
}

impl ServeLaunchOperation {
    pub(crate) fn new(peer: NodeId, launch: LaunchFrame, node: Option<LaunchNode>) -> Self {
        Self {
            peer,
            offer: node.map(|node| (launch, node)),
            admitting: false,
        }
    }
}

impl Operation for ServeLaunchOperation {
    type Output = MetadataTransportMessage;
    type Error = IoError;

    fn first(&mut self) -> LaunchNext {
        let Some((_, node)) = self.offer.as_ref() else {
            return launch_unavailable();
        };
        let read = GetConfigOperation::new(node.realm_id);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> LaunchNext {
        if self.admitting {
            return match result.child::<LaunchDecision, LaunchUndecided>()? {
                Ok(Ok(receipt)) => Ok(Next::Done(MetadataTransportMessage::ForwardedLaunchOffer {
                    result: Ok(Box::new(receipt)),
                })),
                Ok(Err(reason)) => Ok(Next::Done(MetadataTransportMessage::ForwardedLaunchOffer {
                    result: Err(reason),
                })),
                Err(_) => launch_unavailable(),
            };
        }
        let Ok(config) = result.child::<RealmConfigDocument, GetConfigError>()? else {
            return launch_unavailable();
        };
        let eligible = config
            .sync_eligible_nodes()
            .is_ok_and(|nodes| nodes.contains(&self.peer));
        if !eligible {
            return Ok(Next::Done(MetadataTransportMessage::ForwardedLaunchOffer {
                result: Err(LaunchDecline::Unauthorized),
            }));
        }
        let Some((launch, node)) = self.offer.take() else {
            return Err(IoError::Unfinished);
        };
        self.admitting = true;
        let admit = AdmitLaunchOperation::new(launch, node);
        Ok(Next::Call(IoCall::child(admit)))
    }
}

#[cfg(test)]
mod pure_tests {
    use super::*;
    use crate::jobs::job_reads::tests::{child, is_child};
    use crate::tests::records::{Family, node};
    use aruna_core::structs::execution::job::JobRecordBody;

    fn record_request(family: &Family) -> MetadataTransportMessage {
        let spec = family.spec();
        let envelope = family.sign(&family.holder, JobFamilyRecord::Spec(Box::new(spec)));
        MetadataTransportMessage::ForwardJobRecord {
            placement: family.placement,
            record: Box::new(JobRecordFrame::new(envelope).expect("bounded record")),
        }
    }

    fn serving() -> ServingNode {
        Some((node(1), crate::tests::records::REALM))
    }

    // Another message kind is rejected, and without a network nothing is answered for.
    #[test]
    fn unserved_requests_answered() {
        let other = MetadataTransportMessage::ForwardedWriteUnavailable;
        let mut serve = ServeRecordOperation::new(node(2), other, serving(), 5);
        assert!(matches!(
            serve.first(),
            Ok(Next::Done(MetadataTransportMessage::Reject(_)))
        ));
        let family = Family::new([14u8; 32]);
        let mut serve = ServeRecordOperation::new(node(2), record_request(&family), None, 5);
        assert_eq!(
            serve.first().unwrap(),
            Next::Done(MetadataTransportMessage::ForwardedWriteUnavailable)
        );
    }

    // A peer outside the realm's sync set is refused before anything is stored.
    #[test]
    fn outsider_refused() {
        let family = Family::new([15u8; 32]);
        let mut serve = ServeRecordOperation::new(node(9), record_request(&family), serving(), 5);
        assert!(is_child(&serve.first().unwrap()));
        let config = child(Ok::<_, GetConfigError>(family.config.clone()));
        assert_eq!(
            serve.next(config).unwrap(),
            Next::Done(MetadataTransportMessage::ForwardedJobRecord {
                result: Err(JobRecordRejection::Unauthorized),
            })
        );
    }

    // A deleted group refuses the record, while other store failures stay retryable.
    #[test]
    fn append_failures_mapped() {
        assert_eq!(
            append_failure(RecordStoreError::GroupWrite(GroupWriteError::Deleted)),
            ServeError::Refused(JobRecordRejection::Unauthorized)
        );
        assert_eq!(
            append_failure(RecordStoreError::NotFinished),
            ServeError::Unavailable
        );
    }

    // Only a stored record passes; pending and conflicting ones answer the publisher.
    #[test]
    fn admission_verdicts_mapped() {
        assert_eq!(admission_refusal(&Admission::Duplicate), None);
        assert_eq!(
            admission_refusal(&Admission::PendingFull),
            Some(ServeError::Unavailable)
        );
        assert_eq!(
            admission_refusal(&Admission::Conflict),
            Some(ServeError::Refused(JobRecordRejection::Conflict))
        );
    }

    // A stored record with a cancel on file stops the local runs before it is acknowledged.
    #[test]
    fn stored_record_settles() {
        let family = Family::new([16u8; 32]);
        let mut serve = ServeRecordOperation::new(node(2), record_request(&family), serving(), 5);
        serve.accepted = Some(Box::new(Accepted {
            family: family.family(),
            admission: Admission::Duplicate,
            rearms: false,
            paged: None,
        }));
        assert!(is_child(&serve.settle().unwrap()));
        assert_eq!(serve.step, ServeStep::Cancels);
        let spec = family.spec();
        let cancel = family.sign(
            &family.holder,
            JobFamilyRecord::Cancel(family.cancel(&spec)),
        );
        let cancels = child(Ok::<_, FamilyReadError>(vec![cancel]));
        assert!(is_child(&serve.next(cancels).unwrap()));
        assert_eq!(serve.step, ServeStep::Stopped);
        let stopped = child(Ok::<_, IoError>(()));
        assert_eq!(
            serve.next(stopped).unwrap(),
            Next::Done(MetadataTransportMessage::ForwardedJobRecord { result: Ok(()) })
        );
    }

    fn launch_offer(family: &Family) -> LaunchFrame {
        let spec = family.spec();
        let launch = family.launch(&spec, family.holder.public(), 0);
        let envelope = family.sign(&family.holder, JobFamilyRecord::Launch(Box::new(launch)));
        LaunchFrame::new(envelope).expect("bounded launch")
    }

    fn launch_node() -> LaunchNode {
        LaunchNode {
            local: node(1),
            realm_id: crate::tests::records::REALM,
            now_ms: 5,
            execution_id: ulid::Ulid::from_bytes([1u8; 16]),
            limits: None,
        }
    }

    // Without a network an offer stays undecided, and an outsider is declined.
    #[test]
    fn launch_offer_checked() {
        let family = Family::new([17u8; 32]);
        let mut offline = ServeLaunchOperation::new(node(2), launch_offer(&family), None);
        assert_eq!(
            offline.first().unwrap(),
            Next::Done(MetadataTransportMessage::ForwardedWriteUnavailable)
        );
        let node_view = Some(launch_node());
        let mut serve = ServeLaunchOperation::new(node(9), launch_offer(&family), node_view);
        assert!(is_child(&serve.first().unwrap()));
        let config = child(Ok::<_, GetConfigError>(family.config.clone()));
        assert_eq!(
            serve.next(config).unwrap(),
            Next::Done(MetadataTransportMessage::ForwardedLaunchOffer {
                result: Err(LaunchDecline::Unauthorized),
            })
        );
    }

    // An undecided admission answers unavailable so the scheduler can retry.
    #[test]
    fn undecided_launch_unavailable() {
        let family = Family::new([18u8; 32]);
        let mut serve = ServeLaunchOperation::new(
            family.holder.public(),
            launch_offer(&family),
            Some(launch_node()),
        );
        serve.admitting = true;
        let undecided = child(Err::<LaunchDecision, _>(LaunchUndecided));
        assert_eq!(
            serve.next(undecided).unwrap(),
            Next::Done(MetadataTransportMessage::ForwardedWriteUnavailable)
        );
    }

    #[test]
    fn error_rearms_family() {
        // An execution that ended in an infrastructure error decides nothing, so
        // learning it must schedule a witness round again.
        let family = Family::new([13u8; 32]);
        let spec = family.spec();
        let launch = family.launch(&spec, family.holder.public(), 0);
        let receipt = family.receipt(&launch, 1);
        let previous = receipt.digest().expect("receipt digest");
        let running = family.update(&receipt, 0, previous, PhysicalExecutionState::Running, None);
        let errored = family.update(&receipt, 1, previous, PhysicalExecutionState::Error, None);

        assert!(rearms_witness(&family.sign(
            &family.target,
            JobFamilyRecord::Update(Box::new(errored))
        )));
        assert!(!rearms_witness(&family.sign(
            &family.target,
            JobFamilyRecord::Update(Box::new(running))
        )));
        assert!(rearms_witness(&family.sign(
            &family.holder,
            JobFamilyRecord::Claim(family.claim(&spec))
        )));
    }
}
