//! Adapter I/O for the job-record and launch-offer effects.
//!
//! The transport peer is authenticated as a sync-eligible node of this realm.
//! That authority is separate from, and never a substitute for, the publisher
//! signature inside each envelope: a holder that relays a record satisfies no
//! author rule, and a record keeps its original publisher end to end.

use std::sync::Arc;

use aruna_core::NodeId;
use aruna_core::effects::{
    FetchCursor, JobRecordEffect, JobRecordFrame, LaunchFrame, LaunchOfferEffect, PageLimit,
};
use aruna_core::events::{
    JobRecordEvent, JobRecordPage, JobRecordRejection, LaunchDecline, LaunchOfferEvent,
};
use aruna_core::structs::{
    JobFamilyId, JobRecordError, PlacementRef, RealmConfigDocument, RealmId, SubmissionId,
};
use tokio::time::timeout_at;
use tracing::warn;

use super::admit::Admission;
use super::append::{AppendRecordConfig, AppendRecordOperation, RecordOrigin};
use super::audit::{AuditScope, FamilyAuditConfig, FamilyAuditOperation};
use super::rows::PendingNeed;
use crate::driver::{DriverContext, drive};
use crate::metadata::api::load_realm_config;
use crate::metadata::protocol::{JobRecordPageReply, MetadataTransportMessage};
use crate::metadata::transport_message_kind;
use crate::placement::holds_placement;

/// Publishes one record to the family holders, or reads a bounded page back.
pub async fn dispatch_record(context: &DriverContext, effect: JobRecordEffect) -> JobRecordEvent {
    match effect {
        JobRecordEffect::Publish {
            placement,
            holders,
            record,
            deadline,
            ..
        } => {
            let request = MetadataTransportMessage::ForwardJobRecord {
                placement,
                record: Box::new(record.as_ref().clone()),
            };
            publish_record(context, holders.as_slice(), request, deadline).await
        }
        JobRecordEffect::Fetch {
            placement,
            holders,
            submission_id,
            request_digest,
            cursor,
            limit,
            deadline,
            ..
        } => {
            fetch_records(
                context,
                holders.as_slice(),
                PageRequest {
                    placement,
                    submission_id,
                    request_digest,
                    cursor,
                    limit,
                },
                deadline,
            )
            .await
        }
    }
}

struct PageRequest {
    placement: PlacementRef,
    submission_id: SubmissionId,
    request_digest: Option<[u8; 32]>,
    cursor: Option<FetchCursor>,
    limit: PageLimit,
}

/// Asks each holder in turn. A holder that is not a holder of the family may
/// simply be stale, so the next one is asked; a definitive refusal is returned.
async fn publish_record(
    context: &DriverContext,
    holders: &[NodeId],
    request: MetadataTransportMessage,
    deadline: std::time::Duration,
) -> JobRecordEvent {
    let Some(metadata) = context.metadata_handle.as_ref() else {
        return JobRecordEvent::Unavailable("metadata transport unavailable".to_string());
    };
    let deadline = tokio::time::Instant::now() + deadline;
    for holder in holders {
        let reply = match timeout_at(
            deadline,
            metadata.request_forwarded_write(*holder, request.clone()),
        )
        .await
        {
            Ok(Ok(reply)) => reply,
            Ok(Err(error)) => {
                warn!(peer = %holder, error = %error, "Job record publish failed");
                continue;
            }
            Err(_) => break,
        };
        match reply {
            MetadataTransportMessage::ForwardedJobRecord { result: Ok(()) } => {
                return JobRecordEvent::Published { holder: *holder };
            }
            MetadataTransportMessage::ForwardedJobRecord {
                result: Err(JobRecordRejection::NotHolder),
            }
            | MetadataTransportMessage::ForwardedWriteUnavailable => continue,
            MetadataTransportMessage::ForwardedJobRecord {
                result: Err(reason),
            } => {
                return JobRecordEvent::Rejected {
                    holder: *holder,
                    reason,
                };
            }
            other => warn!(
                peer = %holder,
                reply = transport_message_kind(&other),
                "Unexpected job record publish reply"
            ),
        }
    }
    JobRecordEvent::Unavailable("no job family holder accepted the record".to_string())
}

async fn fetch_records(
    context: &DriverContext,
    holders: &[NodeId],
    request: PageRequest,
    deadline: std::time::Duration,
) -> JobRecordEvent {
    let Some(metadata) = context.metadata_handle.as_ref() else {
        return JobRecordEvent::Unavailable("metadata transport unavailable".to_string());
    };
    let deadline = tokio::time::Instant::now() + deadline;
    for holder in holders {
        let message = MetadataTransportMessage::ForwardJobRecordPage {
            placement: request.placement,
            submission_id: request.submission_id,
            request_digest: request.request_digest,
            cursor: request.cursor.clone(),
            limit: request.limit,
        };
        let reply =
            match timeout_at(deadline, metadata.request_forwarded_write(*holder, message)).await {
                Ok(Ok(reply)) => reply,
                Ok(Err(error)) => {
                    warn!(peer = %holder, error = %error, "Job record fetch failed");
                    continue;
                }
                Err(_) => break,
            };
        match reply {
            MetadataTransportMessage::ForwardedJobRecordPage {
                result: Ok(JobRecordPageReply { page, next }),
            } => {
                return JobRecordEvent::Fetched {
                    holder: *holder,
                    records: page,
                    next_cursor: next,
                };
            }
            MetadataTransportMessage::ForwardedJobRecordPage {
                result: Err(reason),
            } => {
                warn!(peer = %holder, reason = ?reason, "Job record holder refused the fetch");
            }
            MetadataTransportMessage::ForwardedWriteUnavailable => continue,
            other => warn!(
                peer = %holder,
                reply = transport_message_kind(&other),
                "Unexpected job record fetch reply"
            ),
        }
    }
    JobRecordEvent::Unavailable("no job family holder answered".to_string())
}

/// Offers one bounded launch to its target. An unreachable or not-yet-admitting
/// target is `Unavailable`: it may still have accepted the launch.
pub async fn dispatch_offer(
    context: &DriverContext,
    effect: LaunchOfferEffect,
) -> LaunchOfferEvent {
    let Some(metadata) = context.metadata_handle.as_ref() else {
        return LaunchOfferEvent::Unavailable("metadata transport unavailable".to_string());
    };
    let target = effect.target.node_id;
    let message = MetadataTransportMessage::ForwardLaunchOffer {
        launch: Box::new(effect.launch.as_ref().clone()),
    };
    let deadline = tokio::time::Instant::now() + effect.deadline;
    match timeout_at(deadline, metadata.request_forwarded_write(target, message)).await {
        Ok(Ok(MetadataTransportMessage::ForwardedLaunchOffer {
            result: Ok(receipt),
        })) => LaunchOfferEvent::Accepted { target, receipt },
        Ok(Ok(MetadataTransportMessage::ForwardedLaunchOffer {
            result: Err(reason),
        })) => LaunchOfferEvent::Declined { target, reason },
        Ok(Ok(other)) => LaunchOfferEvent::Unavailable(format!(
            "target answered a launch offer with {}",
            transport_message_kind(&other)
        )),
        Ok(Err(error)) => LaunchOfferEvent::Unavailable(error.to_string()),
        Err(_) => LaunchOfferEvent::Unavailable("launch offer deadline elapsed".to_string()),
    }
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
        MetadataTransportMessage::ForwardJobRecordPage {
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
                Ok(reply) => MetadataTransportMessage::ForwardedJobRecordPage { result: Ok(reply) },
                Err(ServeError::Refused(reason)) => {
                    MetadataTransportMessage::ForwardedJobRecordPage {
                        result: Err(reason),
                    }
                }
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
    let config = load_realm_config(context.as_ref(), realm_id)
        .await
        .ok_or(ServeError::Unavailable)?;
    let eligible = config
        .sync_eligible_node_ids()
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
    let outcome = drive(
        AppendRecordOperation::new(AppendRecordConfig {
            realm_id: authority.realm_id,
            local_node_id: authority.local,
            record,
            local: None,
            origin: RecordOrigin::Peer(peer),
            now_ms: aruna_core::util::unix_timestamp_millis(),
        }),
        context.as_ref(),
    )
    .await
    .map_err(|error| {
        warn!(error = %error, "Job record append failed");
        ServeError::Unavailable
    })?;
    match outcome.admission {
        // A pending record is durable here and is admitted as soon as its
        // evidence arrives, so the publisher has nothing left to retry.
        Admission::Authentic
        | Admission::Local
        | Admission::Duplicate
        | Admission::Pending(PendingNeed::Evidence(_) | PendingNeed::LocalView) => Ok(()),
        Admission::Conflict => Err(ServeError::Refused(JobRecordRejection::Conflict)),
        Admission::PendingFull => Err(ServeError::Unavailable),
        Admission::Rejected(
            JobRecordError::BadSignature
            | JobRecordError::WrongPublisher(_)
            | JobRecordError::NotHolder(_)
            | JobRecordError::Unauthorized,
        ) => Err(ServeError::Refused(JobRecordRejection::Unauthorized)),
        Admission::Rejected(_) => Err(ServeError::Refused(JobRecordRejection::Invalid)),
    }
}

async fn serve_page(
    context: &Arc<DriverContext>,
    peer: NodeId,
    request: PageRequest,
) -> Result<JobRecordPageReply, ServeError> {
    holder_view(context, peer, request.placement).await?;
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
    Ok(JobRecordPageReply {
        page,
        next: audit.next,
    })
}

/// Serves one inbound launch offer. The offer is bounded and kind-checked at
/// decode; exact admission, the capacity reservation, and the signed receipt
/// are the target's own decision. An undecidable offer is answered as
/// unavailable, never as a refusal: the scheduler must be free to retry it.
pub async fn serve_launch_offer(
    context: &Arc<DriverContext>,
    peer: NodeId,
    launch: LaunchFrame,
) -> MetadataTransportMessage {
    let Some(net_handle) = context.net_handle.as_ref() else {
        return MetadataTransportMessage::ForwardedWriteUnavailable;
    };
    let realm_id = *net_handle.realm_id();
    let Some(config) = load_realm_config(context.as_ref(), realm_id).await else {
        return MetadataTransportMessage::ForwardedWriteUnavailable;
    };
    if !config
        .sync_eligible_node_ids()
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
