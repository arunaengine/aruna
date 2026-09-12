use aruna_core::document::{DocumentSyncEvent, DocumentSyncTarget};
use aruna_core::metadata::{MetadataDocumentLifecycleRecord, MetadataGraphLifecycleRecord};
use aruna_core::structs::{
    MetadataRegistryRecord, PersistentIdMapping, PlacementPolicyDocument, SyncQuarantineIdentity,
};
use tracing::warn;

use crate::document_sync::{
    DocumentSyncDependency, DocumentSyncService, MetadataPlacementOutcome,
    PendingMetadataCreateApply, SyncRejection, node_to_peer,
};
use crate::error::{NetError, Result};

use super::validate::{validate_pid_mapping, validate_policy_document};

pub(super) enum MetadataOutcome {
    Applied {
        target: DocumentSyncTarget,
        tombstone: Option<MetadataGraphLifecycleRecord>,
    },
    Deferred(DocumentSyncDependency),
    Pending(PendingMetadataCreateApply),
    Rejected(SyncRejection),
    Skipped,
}

pub(super) async fn apply_metadata_event(
    service: &DocumentSyncService,
    topic_id: ::irokle::TopicId,
    identity: SyncQuarantineIdentity,
    event: DocumentSyncEvent,
) -> Result<MetadataOutcome> {
    if matches!(event.target(), DocumentSyncTarget::MetadataRegistry { .. }) {
        return apply_registry_event(service, topic_id, identity, event).await;
    }
    if matches!(
        event.target(),
        DocumentSyncTarget::MetadataCreateEvent { .. }
    ) {
        return Ok(match service.prepare_create(identity, event) {
            Ok(pending) => MetadataOutcome::Pending(pending),
            Err(rejection) => {
                warn!(%topic_id, reason = %rejection.reason, "Rejecting malformed metadata create event");
                MetadataOutcome::Rejected(*rejection)
            }
        });
    }
    if matches!(
        event.target(),
        DocumentSyncTarget::MetadataDocumentLifecycle { .. }
    ) {
        return apply_lifecycle_event(service, topic_id, identity, event).await;
    }
    if matches!(
        event.target(),
        DocumentSyncTarget::MetadataGraphLifecycle { .. }
    ) {
        return apply_graph_event(service, topic_id, identity, event).await;
    }
    Err(NetError::Bootstrap(
        "metadata handler received a non-metadata event".to_string(),
    ))
}

pub(super) async fn apply_policy_event(
    service: &DocumentSyncService,
    topic_id: ::irokle::TopicId,
    actor_id: ::irokle::ActorId,
    identity: SyncQuarantineIdentity,
    event: DocumentSyncEvent,
) -> Result<MetadataOutcome> {
    if matches!(
        event.target(),
        DocumentSyncTarget::PersistentIdMapping { .. }
    ) {
        return apply_mapping_event(service, topic_id, actor_id, identity, event).await;
    }
    if matches!(event.target(), DocumentSyncTarget::PlacementPolicy { .. }) {
        return apply_placement_event(service, topic_id, actor_id, identity, event).await;
    }
    Err(NetError::Bootstrap(
        "policy handler received a non-policy event".to_string(),
    ))
}

async fn apply_registry_event(
    service: &DocumentSyncService,
    topic_id: ::irokle::TopicId,
    identity: SyncQuarantineIdentity,
    event: DocumentSyncEvent,
) -> Result<MetadataOutcome> {
    let DocumentSyncEvent::Upsert {
        event_id,
        target:
            DocumentSyncTarget::MetadataRegistry {
                group_id,
                document_id,
            },
        bytes,
        change,
    } = event
    else {
        return Err(NetError::Bootstrap(
            "metadata registry handler received a non-upsert event".to_string(),
        ));
    };
    let target = DocumentSyncTarget::MetadataRegistry {
        group_id,
        document_id,
    };
    let reject = |reason: String| {
        SyncRejection::new(
            identity,
            DocumentSyncEvent::Upsert {
                event_id,
                target: target.clone(),
                bytes: bytes.clone(),
                change,
            },
            reason,
        )
    };
    let record = match postcard::from_bytes::<MetadataRegistryRecord>(&bytes) {
        Ok(record) => record,
        Err(error) => {
            warn!(%topic_id, %document_id, %error, "Rejecting undecodable metadata registry record");
            return Ok(MetadataOutcome::Rejected(reject(format!(
                "undecodable metadata registry record: {error}"
            ))));
        }
    };
    if record.group_id != group_id || record.document_id != document_id {
        warn!(%topic_id, %document_id, "Rejecting metadata registry record whose payload does not match its target");
        return Ok(MetadataOutcome::Rejected(reject(format!(
            "metadata registry target {group_id}/{document_id} does not match payload {}/{}",
            record.group_id, record.document_id
        ))));
    }
    let realm_id = record.realm_id;
    let strategy_id = record.placement.strategy_id;
    let event_bytes = bytes.clone();
    match service.apply_registry_upsert(record, bytes).await? {
        MetadataPlacementOutcome::Accepted(()) => Ok(MetadataOutcome::Applied {
            target,
            tombstone: None,
        }),
        MetadataPlacementOutcome::Deferred(dependency) => {
            warn!(
                %topic_id,
                %realm_id,
                %document_id,
                %strategy_id,
                "Deferring metadata registry record until its placement strategy is available"
            );
            Ok(MetadataOutcome::Deferred(dependency))
        }
        MetadataPlacementOutcome::Rejected => {
            warn!(
                %topic_id,
                %realm_id,
                %document_id,
                %strategy_id,
                "Rejecting metadata registry record with mismatched placement configuration"
            );
            Ok(MetadataOutcome::Rejected(SyncRejection::new(
                identity,
                DocumentSyncEvent::Upsert {
                    event_id,
                    target,
                    bytes: event_bytes,
                    change,
                },
                "metadata registry record has a mismatched placement configuration",
            )))
        }
    }
}

async fn apply_lifecycle_event(
    service: &DocumentSyncService,
    topic_id: ::irokle::TopicId,
    identity: SyncQuarantineIdentity,
    event: DocumentSyncEvent,
) -> Result<MetadataOutcome> {
    let DocumentSyncEvent::Upsert {
        event_id,
        target: DocumentSyncTarget::MetadataDocumentLifecycle { document_id },
        bytes,
        change,
    } = event
    else {
        return Err(NetError::Bootstrap(
            "metadata lifecycle handler received a non-upsert event".to_string(),
        ));
    };
    let target = DocumentSyncTarget::MetadataDocumentLifecycle { document_id };
    let reject = |reason: String| {
        SyncRejection::new(
            identity,
            DocumentSyncEvent::Upsert {
                event_id,
                target: target.clone(),
                bytes: bytes.clone(),
                change,
            },
            reason,
        )
    };
    let lifecycle = match postcard::from_bytes::<MetadataDocumentLifecycleRecord>(&bytes) {
        Ok(lifecycle) => lifecycle,
        Err(error) => {
            warn!(%topic_id, %document_id, %error, "Rejecting undecodable metadata document lifecycle record");
            return Ok(MetadataOutcome::Rejected(reject(format!(
                "undecodable metadata document lifecycle record: {error}"
            ))));
        }
    };
    if lifecycle.document_id() != document_id {
        warn!(%topic_id, %document_id, "Rejecting metadata document lifecycle record whose payload does not match its target");
        return Ok(MetadataOutcome::Rejected(reject(format!(
            "metadata document lifecycle target {document_id} does not match payload document {}",
            lifecycle.document_id()
        ))));
    }
    match lifecycle {
        MetadataDocumentLifecycleRecord::Upsert { event: record } => {
            let record = *record;
            let inner_bytes = postcard::to_allocvec(&record)
                .map_err(|error| NetError::Bootstrap(error.to_string()))?;
            Ok(MetadataOutcome::Pending(PendingMetadataCreateApply {
                identity,
                event: DocumentSyncEvent::Upsert {
                    event_id,
                    target: target.clone(),
                    bytes,
                    change,
                },
                target,
                lifecycle_revision: Some(change),
                record,
                bytes: inner_bytes,
            }))
        }
        MetadataDocumentLifecycleRecord::Delete { event } => {
            let tombstone = event.tombstone.clone();
            let accepted = service
                .apply_document_lifecycle(MetadataDocumentLifecycleRecord::Delete { event }, change)
                .await?;
            if accepted {
                Ok(MetadataOutcome::Applied {
                    target,
                    tombstone: tombstone.is_deleted().then_some(tombstone),
                })
            } else {
                Ok(MetadataOutcome::Skipped)
            }
        }
    }
}

async fn apply_graph_event(
    service: &DocumentSyncService,
    topic_id: ::irokle::TopicId,
    identity: SyncQuarantineIdentity,
    event: DocumentSyncEvent,
) -> Result<MetadataOutcome> {
    let DocumentSyncEvent::Upsert {
        event_id,
        target: DocumentSyncTarget::MetadataGraphLifecycle { graph_iri },
        bytes,
        change,
    } = event
    else {
        return Err(NetError::Bootstrap(
            "metadata graph handler received a non-upsert event".to_string(),
        ));
    };
    let target = DocumentSyncTarget::MetadataGraphLifecycle {
        graph_iri: graph_iri.clone(),
    };
    let reject = |reason: String| {
        SyncRejection::new(
            identity,
            DocumentSyncEvent::Upsert {
                event_id,
                target: target.clone(),
                bytes: bytes.clone(),
                change,
            },
            reason,
        )
    };
    let record = match postcard::from_bytes::<MetadataGraphLifecycleRecord>(&bytes) {
        Ok(record) => record,
        Err(error) => {
            warn!(%topic_id, %graph_iri, %error, "Rejecting undecodable metadata graph lifecycle record");
            return Ok(MetadataOutcome::Rejected(reject(format!(
                "undecodable metadata graph lifecycle record: {error}"
            ))));
        }
    };
    if record.graph_iri != graph_iri {
        warn!(%topic_id, %graph_iri, "Rejecting metadata graph lifecycle record whose payload does not match its target");
        return Ok(MetadataOutcome::Rejected(reject(format!(
            "metadata graph lifecycle target `{graph_iri}` does not match payload graph `{}`",
            record.graph_iri
        ))));
    }
    let accepted = service.apply_graph_lifecycle(record.clone(), bytes).await?;
    if accepted {
        let tombstone = record.is_deleted().then_some(record);
        Ok(MetadataOutcome::Applied { target, tombstone })
    } else {
        Ok(MetadataOutcome::Skipped)
    }
}

async fn apply_mapping_event(
    service: &DocumentSyncService,
    topic_id: ::irokle::TopicId,
    actor_id: ::irokle::ActorId,
    identity: SyncQuarantineIdentity,
    event: DocumentSyncEvent,
) -> Result<MetadataOutcome> {
    let DocumentSyncEvent::Upsert {
        event_id,
        target: DocumentSyncTarget::PersistentIdMapping { document_id },
        bytes,
        change,
    } = event
    else {
        return reject_mapping_event(topic_id, identity, event);
    };
    let target = DocumentSyncTarget::PersistentIdMapping { document_id };
    let reject = |reason: String| {
        SyncRejection::new(
            identity,
            DocumentSyncEvent::Upsert {
                event_id,
                target: target.clone(),
                bytes: bytes.clone(),
                change,
            },
            reason,
        )
    };
    let mapping = match PersistentIdMapping::from_bytes(&bytes) {
        Ok(mapping) => mapping,
        Err(error) => {
            warn!(%topic_id, %document_id, %error, "Rejecting undecodable persistent id mapping");
            return Ok(MetadataOutcome::Rejected(reject(format!(
                "undecodable persistent id mapping: {error}"
            ))));
        }
    };
    if let Err(reason) = validate_pid_mapping(document_id, &mapping, &change) {
        warn!(%topic_id, %document_id, %reason, "Rejecting invalid persistent id mapping");
        return Ok(MetadataOutcome::Rejected(reject(format!(
            "invalid persistent id mapping: {reason}"
        ))));
    }
    // The revision actor is the only publisher of its transition.
    let expected_actor = ::irokle::actor_id_for(topic_id, node_to_peer(&mapping.revision.actor));
    if actor_id != expected_actor {
        warn!(
            %topic_id,
            %document_id,
            "Rejecting persistent id mapping whose revision actor is not its publisher"
        );
        return Ok(MetadataOutcome::Rejected(reject(
            "persistent id mapping revision actor is not its publisher".to_string(),
        )));
    }
    match service
        .apply_pid_mapping(&mapping, change.placement)
        .await?
    {
        MetadataPlacementOutcome::Accepted(true) => Ok(MetadataOutcome::Applied {
            target,
            tombstone: None,
        }),
        MetadataPlacementOutcome::Accepted(false) => Ok(MetadataOutcome::Skipped),
        MetadataPlacementOutcome::Deferred(dependency) => {
            warn!(
                %topic_id,
                %document_id,
                "Deferring persistent id mapping until its placement configuration is available"
            );
            Ok(MetadataOutcome::Deferred(dependency))
        }
        MetadataPlacementOutcome::Rejected => {
            warn!(
                %topic_id,
                %document_id,
                "Rejecting persistent id mapping stamped with a placement its document id does not decode to"
            );
            Ok(MetadataOutcome::Rejected(reject(
                "persistent id mapping has a mismatched placement configuration".to_string(),
            )))
        }
    }
}

async fn apply_placement_event(
    service: &DocumentSyncService,
    topic_id: ::irokle::TopicId,
    actor_id: ::irokle::ActorId,
    identity: SyncQuarantineIdentity,
    event: DocumentSyncEvent,
) -> Result<MetadataOutcome> {
    let DocumentSyncEvent::Upsert {
        event_id,
        target: DocumentSyncTarget::PlacementPolicy { policy_id },
        bytes,
        change,
    } = event
    else {
        return reject_policy_event(topic_id, identity, event);
    };
    let target = DocumentSyncTarget::PlacementPolicy { policy_id };
    let reject = |reason: String| {
        SyncRejection::new(
            identity,
            DocumentSyncEvent::Upsert {
                event_id,
                target: target.clone(),
                bytes: bytes.clone(),
                change,
            },
            reason,
        )
    };
    let document = match postcard::from_bytes::<PlacementPolicyDocument>(&bytes) {
        Ok(document) => document,
        Err(error) => {
            warn!(%topic_id, %policy_id, %error, "Rejecting undecodable placement policy document");
            return Ok(MetadataOutcome::Rejected(reject(format!(
                "undecodable placement policy document: {error}"
            ))));
        }
    };
    if let Err(reason) = validate_policy_document(policy_id, service.realm_id, &document, &change) {
        warn!(%topic_id, %policy_id, %reason, "Rejecting invalid placement policy document");
        return Ok(MetadataOutcome::Rejected(reject(format!(
            "invalid placement policy document: {reason}"
        ))));
    }
    // The stored actor is the original publisher, including through a relay.
    let expected_actor =
        ::irokle::actor_id_for(topic_id, node_to_peer(&document.publication.publisher));
    if actor_id != expected_actor {
        warn!(
            %topic_id,
            %policy_id,
            "Rejecting placement policy document whose actor is not its publisher"
        );
        return Ok(MetadataOutcome::Rejected(reject(
            "placement policy actor is not its publisher".to_string(),
        )));
    }
    match service
        .apply_policy_document(&document, change.placement)
        .await?
    {
        MetadataPlacementOutcome::Accepted(true) => Ok(MetadataOutcome::Applied {
            target,
            tombstone: None,
        }),
        MetadataPlacementOutcome::Accepted(false) => Ok(MetadataOutcome::Skipped),
        MetadataPlacementOutcome::Deferred(dependency) => {
            warn!(
                %topic_id,
                %policy_id,
                "Deferring placement policy document until its placement configuration is available"
            );
            Ok(MetadataOutcome::Deferred(dependency))
        }
        MetadataPlacementOutcome::Rejected => {
            warn!(
                %topic_id,
                %policy_id,
                "Rejecting placement policy document with a mismatched placement or reused id"
            );
            Ok(MetadataOutcome::Rejected(reject(
                "placement policy document has a mismatched placement or reuses its id".to_string(),
            )))
        }
    }
}

fn reject_mapping_event(
    topic_id: ::irokle::TopicId,
    identity: SyncQuarantineIdentity,
    event: DocumentSyncEvent,
) -> Result<MetadataOutcome> {
    // The mapping row is a permanent tombstone and only accepts upserts.
    warn!(
        %topic_id,
        target = ?event.target(),
        "Skipping unsupported non-upsert persistent id mapping event"
    );
    Ok(MetadataOutcome::Rejected(SyncRejection::new(
        identity,
        event,
        "unsupported non-upsert persistent id mapping event",
    )))
}

fn reject_policy_event(
    topic_id: ::irokle::TopicId,
    identity: SyncQuarantineIdentity,
    event: DocumentSyncEvent,
) -> Result<MetadataOutcome> {
    // A policy document is immutable and only accepts its initial upsert.
    warn!(
        %topic_id,
        target = ?event.target(),
        "Skipping unsupported non-upsert placement policy event"
    );
    Ok(MetadataOutcome::Rejected(SyncRejection::new(
        identity,
        event,
        "unsupported non-upsert placement policy event",
    )))
}
