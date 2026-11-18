//! Serves job record and launch offer messages from other realm nodes.
//! A relaying peer is authenticated on its own and is never treated as the record author.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::NodeId;
use aruna_core::effects::{FetchCursor, JobRecordFrame, LaunchFrame, PageLimit};
use aruna_core::events::{JobRecordPage, JobRecordRejection, LaunchDecline};
use aruna_core::structs::execution::job::{
    JobFamilyId, JobFamilyRecord, JobRecordEnvelope, JobRecordError, JobRecordKind,
    PhysicalExecutionState, SubmissionId,
};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::placement::record::PlacementRef;
use tracing::warn;

use super::admit::Admission;
use super::append::{AppendRecordConfig, AppendRecordOperation, RecordOrigin};
use super::audit::{AuditScope, FamilyAuditConfig, FamilyAuditOperation};
use super::family::LoadFamilyOperation;
use crate::document_repository::GetConfigOperation;
use crate::driver::{DriverContext, drive};
use crate::metadata::protocol::{JobPageReply, MetadataTransportMessage};
use crate::metadata::transport_message_kind;
use crate::realm::load_config::config_unavailable;
use aruna_core::placement::holds_placement;
use aruna_core::structs::execution::job::record_rows::PendingNeed;

struct PageRequest {
    placement: PlacementRef,
    submission_id: SubmissionId,
    request_digest: Option<[u8; 32]>,
    cursor: Option<FetchCursor>,
    limit: PageLimit,
}

/// Serves one inbound record publish or page request from an authenticated peer.
pub async fn serve_job_record(
    context: &Arc<DriverContext>,
    peer: NodeId,
    message: MetadataTransportMessage,
) -> MetadataTransportMessage {
    match message {
        MetadataTransportMessage::ForwardJobRecord { placement, record } => {
            match accept_record(context, peer, placement, *record).await {
                Ok(()) => MetadataTransportMessage::ForwardedJobRecord { result: Ok(()) },
                Err(ServeError::Refused(reason)) => MetadataTransportMessage::ForwardedJobRecord {
                    result: Err(reason),
                },
                Err(ServeError::Unavailable) => MetadataTransportMessage::ForwardedWriteUnavailable,
            }
        }
        MetadataTransportMessage::ForwardRecordPage {
            placement,
            submission_id,
            request_digest,
            cursor,
            limit,
        } => {
            let request = PageRequest {
                placement,
                submission_id,
                request_digest,
                cursor,
                limit,
            };
            match serve_page(context, peer, request).await {
                Ok(reply) => MetadataTransportMessage::ForwardedRecordPage { result: Ok(reply) },
                Err(ServeError::Refused(reason)) => MetadataTransportMessage::ForwardedRecordPage {
                    result: Err(reason),
                },
                Err(ServeError::Unavailable) => MetadataTransportMessage::ForwardedWriteUnavailable,
            }
        }
        other => MetadataTransportMessage::Reject(format!(
            "unexpected job record request {}",
            transport_message_kind(&other)
        )),
    }
}

/// A refusal this node stands behind, or a local failure that says nothing
/// about the record. A transient local failure must never be answered as a
/// definitive refusal: the publisher would stop retrying a valid record.
enum ServeError {
    Refused(JobRecordRejection),
    Unavailable,
}

/// This node's authority to answer for one family placement: it must hold the
/// placement, and the peer must be a sync-eligible node of this realm.
struct ServeAuthority {
    config: RealmConfigDocument,
    local: NodeId,
    realm_id: RealmId,
}

async fn holder_view(
    context: &Arc<DriverContext>,
    peer: NodeId,
    placement: PlacementRef,
) -> Result<ServeAuthority, ServeError> {
    let net_handle = context.net_handle.as_ref().ok_or(ServeError::Unavailable)?;
    let realm_id = *net_handle.realm_id();
    let config = drive(GetConfigOperation::new(realm_id), context.as_ref())
        .await
        .inspect_err(config_unavailable)
        .ok()
        .ok_or(ServeError::Unavailable)?;
    let eligible = config
        .sync_eligible_nodes()
        .is_ok_and(|nodes| nodes.contains(&peer));
    if !eligible {
        return Err(ServeError::Refused(JobRecordRejection::Unauthorized));
    }
    let local = net_handle.node_id();
    if !holds_placement(&config, &placement, local) {
        return Err(ServeError::Refused(JobRecordRejection::NotHolder));
    }
    Ok(ServeAuthority {
        config,
        local,
        realm_id,
    })
}

async fn accept_record(
    context: &Arc<DriverContext>,
    peer: NodeId,
    placement: PlacementRef,
    record: JobRecordFrame,
) -> Result<(), ServeError> {
    let authority = holder_view(context, peer, placement).await?;
    let family = record.envelope().family();
    // The family placement is derived here, never taken from the requester.
    let derived = authority
        .config
        .family_placement(family.submission_id)
        .map_err(|_| ServeError::Refused(JobRecordRejection::NotHolder))?;
    if derived != placement {
        return Err(ServeError::Refused(JobRecordRejection::Invalid));
    }
    let rearms = rearms_witness(record.envelope());
    let paged = binds_pages(&record.envelope().record).then(|| record.envelope().record.clone());
    let now_ms = aruna_core::time::unix_timestamp_millis();
    let outcome = drive(
        AppendRecordOperation::new(AppendRecordConfig {
            realm_id: authority.realm_id,
            local_node_id: authority.local,
            record,
            local: None,
            origin: RecordOrigin::Peer(peer),
            now_ms,
        }),
        context.as_ref(),
    )
    .await
    .map_err(|error| {
        if matches!(
            error,
            super::RecordStoreError::GroupWrite(
                aruna_core::structs::identity::group_delete::GroupWriteError::Deleted
            )
        ) {
            return ServeError::Refused(JobRecordRejection::Unauthorized);
        }
        warn!(error = %error, "Job record append failed");
        ServeError::Unavailable
    })?;
    if matches!(outcome.admission, Admission::Authentic) {
        context.notify_dashboard_change();
        if rearms {
            crate::jobs::lifecycle::witness::arm_family(context.as_ref(), family, now_ms).await;
        }
    }
    match outcome.admission {
        Admission::Authentic | Admission::Duplicate => {
            // The publisher retries until this holder keeps the pages the record binds.
            if let Some(record) = &paged {
                super::pages::keep_pages(context.as_ref(), record, peer)
                    .await
                    .map_err(|error| {
                        warn!(error = %error, "Job record input pages unavailable");
                        ServeError::Unavailable
                    })?;
            }
            let cancels = drive(
                LoadFamilyOperation::kind(family, JobRecordKind::Cancel),
                context.as_ref(),
            )
            .await
            .map_err(|_| ServeError::Unavailable)?;
            // Admission may promote a pending cancel, and retries must reapply its side effect.
            if !cancels.is_empty() {
                crate::jobs::lifecycle::cancel::cancel_local_runs(context.as_ref(), family).await;
            }
            Ok(())
        }
        Admission::Local
        | Admission::Pending(
            PendingNeed::Evidence(_) | PendingNeed::LocalView | PendingNeed::HolderView,
        )
        | Admission::PendingFull => Err(ServeError::Unavailable),
        Admission::Conflict => Err(ServeError::Refused(JobRecordRejection::Conflict)),
        Admission::Rejected(
            JobRecordError::BadSignature
            | JobRecordError::WrongPublisher(_)
            | JobRecordError::NotHolder(_)
            | JobRecordError::Unauthorized,
        ) => Err(ServeError::Refused(JobRecordRejection::Unauthorized)),
        Admission::Rejected(_) => Err(ServeError::Refused(JobRecordRejection::Invalid)),
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

async fn serve_page(
    context: &Arc<DriverContext>,
    peer: NodeId,
    request: PageRequest,
) -> Result<JobPageReply, ServeError> {
    let authority = holder_view(context, peer, request.placement).await?;
    let derived = authority
        .config
        .family_placement(request.submission_id)
        .map_err(|_| ServeError::Refused(JobRecordRejection::NotHolder))?;
    if derived != request.placement {
        return Err(ServeError::Refused(JobRecordRejection::Invalid));
    }
    let scope = match request.request_digest {
        Some(request_digest) => AuditScope::Family(JobFamilyId {
            submission_id: request.submission_id,
            request_digest,
        }),
        None => AuditScope::Submission(request.submission_id),
    };
    let audit = drive(
        FamilyAuditOperation::new(FamilyAuditConfig {
            scope,
            cursor: request.cursor,
            limit: request.limit,
        }),
        context.as_ref(),
    )
    .await
    .map_err(|error| {
        warn!(error = %error, "Job record page read failed");
        ServeError::Unavailable
    })?;
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

/// Serves one inbound launch offer. The offer is bounded and kind-checked at
/// decode; admission, reservation, and receipt are the target's own decision.
/// An undecidable offer is answered as unavailable so the scheduler can retry.
pub async fn serve_launch_offer(
    context: &Arc<DriverContext>,
    peer: NodeId,
    launch: LaunchFrame,
) -> MetadataTransportMessage {
    let Some(net_handle) = context.net_handle.as_ref() else {
        return MetadataTransportMessage::ForwardedWriteUnavailable;
    };
    let realm_id = *net_handle.realm_id();
    let Some(config) = drive(GetConfigOperation::new(realm_id), context.as_ref())
        .await
        .inspect_err(config_unavailable)
        .ok()
    else {
        return MetadataTransportMessage::ForwardedWriteUnavailable;
    };
    if !config
        .sync_eligible_nodes()
        .is_ok_and(|nodes| nodes.contains(&peer))
    {
        return MetadataTransportMessage::ForwardedLaunchOffer {
            result: Err(LaunchDecline::Unauthorized),
        };
    }
    match crate::jobs::lifecycle::target::admit_launch(context, launch).await {
        Some(Ok(receipt)) => MetadataTransportMessage::ForwardedLaunchOffer {
            result: Ok(Box::new(receipt)),
        },
        Some(Err(reason)) => MetadataTransportMessage::ForwardedLaunchOffer {
            result: Err(reason),
        },
        None => MetadataTransportMessage::ForwardedWriteUnavailable,
    }
}

#[cfg(test)]
mod pure_tests {
    use super::*;
    use crate::tests::records::Family;
    use aruna_core::structs::execution::job::JobRecordBody;

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
