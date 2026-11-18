//! Decides what one replicated non-admin document event does to the stored rows.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use byteview::ByteView;
use ulid::Ulid;

use super::create::{CreateEventPlan, PendingCreateApply};
use super::links::LinkPlan;
use super::reads::{group_authorization, realm_authorization, realm_config, relay_allowed};
use super::registry::{
    DeleteRegistryPlan, DocumentLifecyclePlan, GraphLifecyclePlan, PidMappingPlan,
    PolicyDocumentPlan, RegistryUpsertPlan, Row, revision_entries, validate_metadata_event,
};
use super::{ApplyError, PlacementOutcome, Step, invalid};
use crate::deferred_topics::DocumentSyncDependency;
use crate::document::{DocumentChange, DocumentChangeKind, DocumentEvent, DocumentTarget};
use crate::git::{GitPackRecord, GitRecord};
use crate::keyspaces::{
    SYNC_REVISION_KEYSPACE, VAULT_RETIRED_KEYSPACE, VAULT_REVISION_KEYSPACE,
    WATCH_INTEREST_KEYSPACE,
};
use crate::metadata::{GraphLifecycleRecord, MetadataEventRecord, MetadataLifecycleRecord};
use crate::operation::TransactionMode;
use crate::reducer::reduced_admin_target;
use crate::register::{Admission, admit_write};
use crate::row_plan::{PlanRows, PlanStop, Planned, RowPlan};
use crate::storage_entries::sync_revision_key;
use crate::structs::execution::notification_watch::interest_dirty_key;
use crate::structs::execution::setting::SettingChannel;
use crate::structs::identity::realm::RealmId;
use crate::structs::identity::user::vault::{head_rows, record_rows, user_record_key};
use crate::structs::placement::policy::document::{
    PlacementPolicyDocument, verify_policy_authority,
};
use crate::structs::placement::record::PlacementRef;
use crate::structs::storage::metadata_registry::MetadataRegistryRecord;
use crate::structs::{PersistentIdMapping, SyncQuarantineIdentity, SyncRejection};
use crate::sync_checks::{
    check_vault_record, node_info_supersedes, published_by, shared_verdict, validate_link,
    validate_node_upsert, validate_pid_mapping, validate_policy_document, validate_register_upsert,
    validate_usage_upsert, validate_watch_delete, validate_watch_interest, validate_watch_upsert,
    watch_change_applies,
};

/// What one replicated event did.
#[derive(Debug, PartialEq)]
pub enum ApplyOutcome {
    Applied {
        target: DocumentTarget,
        tombstone: Option<Box<GraphLifecycleRecord>>,
        /// The document whose Git history gained a record.
        git_document: Option<Ulid>,
    },
    Deferred(DocumentSyncDependency),
    Pending(Box<PendingCreateApply>),
    Rejected(Box<SyncRejection>),
    Skipped,
}

impl ApplyOutcome {
    fn applied(target: DocumentTarget) -> Self {
        Self::Applied {
            target,
            tombstone: None,
            git_document: None,
        }
    }
}

/// Applies one replicated event that is not an admin operation.
#[derive(Clone, Debug, PartialEq)]
pub struct ApplyEventPlan {
    pub topic_id: irokle::TopicId,
    pub actor_id: irokle::ActorId,
    pub realm_id: RealmId,
    pub identity: SyncQuarantineIdentity,
    pub event: DocumentEvent,
}

type EventStep = Step<Planned<ApplyOutcome>>;

impl ApplyEventPlan {
    fn reject(&self, reason: impl Into<String>) -> EventStep {
        let rejection = SyncRejection::new(self.identity, self.event.clone(), reason.into());
        Ok(Planned::output(ApplyOutcome::Rejected(Box::new(rejection))))
    }

    fn placed(
        &self,
        outcome: PlacementOutcome<bool>,
        deletes: Vec<(String, ByteView)>,
        writes: Vec<Row>,
        mismatch: &str,
    ) -> EventStep {
        let target = self.event.target().clone();
        Ok(match outcome {
            PlacementOutcome::Accepted(true) => {
                Planned::changes(ApplyOutcome::applied(target), deletes, writes)
            }
            PlacementOutcome::Accepted(false) => Planned::output(ApplyOutcome::Skipped),
            PlacementOutcome::Deferred(dependency) => {
                Planned::output(ApplyOutcome::Deferred(dependency))
            }
            PlacementOutcome::Rejected => return self.reject(mismatch),
        })
    }

    fn watch(&self, rows: &mut PlanRows) -> EventStep {
        let by_publisher = |change: &DocumentChange| {
            published_by(self.topic_id, self.actor_id, &change.current.actor)
        };
        match &self.event {
            DocumentEvent::Upsert {
                target: target @ DocumentTarget::WatchSubscription { .. },
                bytes,
                change,
                ..
            } => {
                if !by_publisher(change) {
                    return self.reject("watch subscription revision actor is not its publisher");
                }
                if let Err(reason) = validate_watch_upsert(target, bytes, change) {
                    return self.reject(format!("invalid watch subscription: {reason}"));
                }
                self.watch_change(rows, target, Some(bytes), change)
            }
            DocumentEvent::Delete {
                target: target @ DocumentTarget::WatchSubscription { .. },
                change,
                ..
            } => {
                if !by_publisher(change) {
                    return self.reject("watch subscription delete actor is not its publisher");
                }
                if let Err(reason) = validate_watch_delete(target, change) {
                    return self.reject(format!("invalid watch subscription delete: {reason}"));
                }
                self.watch_change(rows, target, None, change)
            }
            DocumentEvent::Upsert {
                target,
                bytes,
                change,
                ..
            } if register_target(target) => {
                if !by_publisher(change) {
                    return self.reject("notification setting revision actor is not its publisher");
                }
                if let Err(reason) = validate_register_upsert(target, bytes, change) {
                    return self.reject(format!("invalid notification setting: {reason}"));
                }
                // Register writes are immutable: a replay changes nothing, other bytes are refused.
                let stored = rows.read(target.storage_keyspace(), target.storage_key())?;
                match admit_write(stored.as_deref(), bytes) {
                    Admission::Store => self.watch_change(rows, target, Some(bytes), change),
                    Admission::Replay => Ok(Planned::output(ApplyOutcome::Skipped)),
                    Admission::Conflict => {
                        self.reject("notification setting conflicts with a stored write")
                    }
                }
            }
            // Settings are immutable register writes; a newer write turns a setting off.
            event @ DocumentEvent::Delete { .. } if register_target(event.target()) => {
                self.reject("notification settings are never deleted")
            }
            _ => Err(
                ApplyError::Invalid("watch handler received a non-watch event".to_string()).into(),
            ),
        }
    }

    fn watch_change(
        &self,
        rows: &mut PlanRows,
        target: &DocumentTarget,
        bytes: Option<&Vec<u8>>,
        change: &DocumentChange,
    ) -> EventStep {
        let Some((deletes, writes)) = watch_rows(rows, target, bytes, change)? else {
            return Ok(Planned::output(ApplyOutcome::Skipped));
        };
        Ok(Planned::changes(
            ApplyOutcome::applied(target.clone()),
            deletes,
            writes,
        ))
    }

    fn shared(&self, rows: &mut PlanRows) -> EventStep {
        if let Err(reason) = shared_verdict(self.topic_id, self.actor_id, &self.event) {
            return self.reject(reason);
        }
        let DocumentEvent::Upsert { target, bytes, .. } = &self.event else {
            return Err(ApplyError::Invalid(
                "shared realm document verdict admitted a non-upsert event".to_string(),
            )
            .into());
        };
        let writes = plain_upsert(rows, target, bytes)?;
        Ok(Planned::changes(
            ApplyOutcome::applied(target.clone()),
            Vec::new(),
            writes,
        ))
    }

    fn registry(&self, rows: &mut PlanRows) -> EventStep {
        let DocumentEvent::Upsert {
            target:
                target @ DocumentTarget::MetadataRegistry {
                    group_id,
                    document_id,
                },
            bytes,
            ..
        } = &self.event
        else {
            return Err(ApplyError::Invalid(
                "metadata registry handler received a non-upsert event".to_string(),
            )
            .into());
        };
        let record = match postcard::from_bytes::<MetadataRegistryRecord>(bytes) {
            Ok(record) => record,
            Err(error) => {
                return self.reject(format!("undecodable metadata registry record: {error}"));
            }
        };
        if record.group_id != *group_id || record.document_id != *document_id {
            return self.reject(format!(
                "metadata registry target {group_id}/{document_id} does not match payload {}/{}",
                record.group_id, record.document_id
            ));
        }
        let plan = RegistryUpsertPlan {
            record,
            primary_bytes: bytes.clone(),
        };
        let Planned {
            output,
            deletes,
            writes,
        } = plan.plan(rows)?;
        Ok(match output {
            PlacementOutcome::Accepted(()) => {
                Planned::changes(ApplyOutcome::applied(target.clone()), deletes, writes)
            }
            PlacementOutcome::Deferred(dependency) => {
                Planned::output(ApplyOutcome::Deferred(dependency))
            }
            PlacementOutcome::Rejected => {
                return self
                    .reject("metadata registry record has a mismatched placement configuration");
            }
        })
    }

    fn create_event(&self) -> EventStep {
        let DocumentEvent::Upsert {
            target:
                DocumentTarget::MetadataCreateEvent {
                    document_id,
                    event_id,
                },
            bytes,
            ..
        } = &self.event
        else {
            return Err(ApplyError::Invalid(
                "metadata create handler received another event".to_string(),
            )
            .into());
        };
        let record = match postcard::from_bytes::<MetadataEventRecord>(bytes) {
            Ok(record) => record,
            Err(error) => {
                return self.reject(format!("undecodable metadata create event: {error}"));
            }
        };
        if record.record.document_id != *document_id || record.event_id != *event_id {
            return self.reject(format!(
                "metadata create-event target {document_id}/{event_id} does not match payload {}/{}",
                record.record.document_id, record.event_id
            ));
        }
        Ok(Planned::output(ApplyOutcome::Pending(Box::new(
            PendingCreateApply {
                identity: self.identity,
                event: self.event.clone(),
                target: self.event.target().clone(),
                record,
                bytes: bytes.clone(),
                lifecycle_revision: None,
                message: None,
            },
        ))))
    }

    fn lifecycle(&self, rows: &mut PlanRows) -> EventStep {
        let DocumentEvent::Upsert {
            target: target @ DocumentTarget::MetadataDocumentLifecycle { document_id },
            bytes,
            change,
            ..
        } = &self.event
        else {
            return Err(ApplyError::Invalid(
                "metadata lifecycle handler received a non-upsert event".to_string(),
            )
            .into());
        };
        let lifecycle = match postcard::from_bytes::<MetadataLifecycleRecord>(bytes) {
            Ok(lifecycle) => lifecycle,
            Err(error) => {
                return self.reject(format!(
                    "undecodable metadata document lifecycle record: {error}"
                ));
            }
        };
        if lifecycle.document_id() != *document_id {
            return self.reject(format!(
                "metadata document lifecycle target {document_id} does not match payload document {}",
                lifecycle.document_id()
            ));
        }
        let message = lifecycle.message().map(str::to_owned);
        match lifecycle {
            MetadataLifecycleRecord::Upsert { event: record }
            | MetadataLifecycleRecord::UpsertWithMessage { event: record, .. } => {
                let record = *record;
                let inner = postcard::to_allocvec(&record).map_err(invalid)?;
                Ok(Planned::output(ApplyOutcome::Pending(Box::new(
                    PendingCreateApply {
                        identity: self.identity,
                        event: self.event.clone(),
                        target: target.clone(),
                        lifecycle_revision: Some(*change),
                        record,
                        bytes: inner,
                        message,
                    },
                ))))
            }
            MetadataLifecycleRecord::Delete { event } => {
                let tombstone = event.tombstone.clone();
                let plan = DocumentLifecyclePlan {
                    record: MetadataLifecycleRecord::Delete { event },
                    change: *change,
                };
                let Planned {
                    output,
                    deletes,
                    writes,
                } = plan.plan(rows)?;
                Ok(match output {
                    true => Planned::changes(
                        ApplyOutcome::Applied {
                            target: target.clone(),
                            tombstone: tombstone.is_deleted().then_some(Box::new(tombstone)),
                            git_document: None,
                        },
                        deletes,
                        writes,
                    ),
                    false => Planned::output(ApplyOutcome::Skipped),
                })
            }
        }
    }

    fn graph(&self, rows: &mut PlanRows) -> EventStep {
        let DocumentEvent::Upsert {
            target: target @ DocumentTarget::MetadataGraphLifecycle { graph_iri },
            bytes,
            ..
        } = &self.event
        else {
            return Err(ApplyError::Invalid(
                "metadata graph handler received a non-upsert event".to_string(),
            )
            .into());
        };
        let record = match postcard::from_bytes::<GraphLifecycleRecord>(bytes) {
            Ok(record) => record,
            Err(error) => {
                return self.reject(format!(
                    "undecodable metadata graph lifecycle record: {error}"
                ));
            }
        };
        if record.graph_iri != *graph_iri {
            return self.reject(format!(
                "metadata graph lifecycle target `{graph_iri}` does not match payload graph `{}`",
                record.graph_iri
            ));
        }
        let plan = GraphLifecyclePlan {
            record: record.clone(),
            primary_bytes: bytes.clone(),
        };
        let Planned {
            output,
            deletes,
            writes,
        } = plan.plan(rows)?;
        Ok(match output {
            true => Planned::changes(
                ApplyOutcome::Applied {
                    target: target.clone(),
                    tombstone: record.is_deleted().then_some(Box::new(record)),
                    git_document: None,
                },
                deletes,
                writes,
            ),
            false => Planned::output(ApplyOutcome::Skipped),
        })
    }

    fn mapping(&self, rows: &mut PlanRows) -> EventStep {
        let DocumentEvent::Upsert {
            target: DocumentTarget::PersistentIdMapping { document_id },
            bytes,
            change,
            ..
        } = &self.event
        else {
            // The mapping row is a permanent tombstone and only accepts upserts.
            return self.reject("unsupported non-upsert persistent id mapping event");
        };
        let mapping = match PersistentIdMapping::from_bytes(bytes) {
            Ok(mapping) => mapping,
            Err(error) => {
                return self.reject(format!("undecodable persistent id mapping: {error}"));
            }
        };
        if let Err(reason) = validate_pid_mapping(*document_id, &mapping, change) {
            return self.reject(format!("invalid persistent id mapping: {reason}"));
        }
        // The revision actor is the only publisher of its transition.
        if !published_by(self.topic_id, self.actor_id, &mapping.revision.actor) {
            return self.reject("persistent id mapping revision actor is not its publisher");
        }
        let plan = PidMappingPlan {
            realm_id: self.realm_id,
            incoming: mapping,
            placement: change.placement,
        };
        let Planned {
            output,
            deletes,
            writes,
        } = plan.plan(rows)?;
        self.placed(
            output,
            deletes,
            writes,
            "persistent id mapping has a mismatched placement configuration",
        )
    }

    fn policy(&self, rows: &mut PlanRows) -> EventStep {
        let DocumentEvent::Upsert {
            target: DocumentTarget::PlacementPolicy { policy_id },
            bytes,
            change,
            ..
        } = &self.event
        else {
            // A policy document is immutable and only accepts its initial upsert.
            return self.reject("unsupported non-upsert placement policy event");
        };
        let document = match postcard::from_bytes::<PlacementPolicyDocument>(bytes) {
            Ok(document) => document,
            Err(error) => {
                return self.reject(format!("undecodable placement policy document: {error}"));
            }
        };
        if let Err(reason) = validate_policy_document(*policy_id, self.realm_id, &document, change)
        {
            return self.reject(format!("invalid placement policy document: {reason}"));
        }
        // The stored actor is the original publisher, including through a relay.
        if !published_by(
            self.topic_id,
            self.actor_id,
            &document.publication.publisher,
        ) {
            return self.reject("placement policy actor is not its publisher");
        }
        let Planned {
            output,
            deletes,
            writes,
        } = policy_change(rows, self.realm_id, &document, change.placement)?;
        self.placed(
            output,
            deletes,
            writes,
            "placement policy document has a mismatched placement or reuses its id",
        )
    }

    fn link(&self, rows: &mut PlanRows) -> EventStep {
        let (target, bytes, change) = match &self.event {
            DocumentEvent::Upsert {
                target,
                bytes,
                change,
                ..
            } => (target, Some(bytes.clone()), *change),
            DocumentEvent::Delete { target, change, .. } => (target, None, *change),
            DocumentEvent::AdminOperation { .. } => {
                return self.reject("repository link events are upserts or deletes");
            }
        };
        if !published_by(self.topic_id, self.actor_id, &change.current.actor) {
            return self.reject("repository link revision actor is not its publisher");
        }
        if let Err(reason) = validate_link(target, bytes.as_deref(), &change) {
            return self.reject(format!("invalid repository link: {reason}"));
        }
        let plan = LinkPlan {
            realm_id: self.realm_id,
            target: target.clone(),
            bytes,
            change,
        };
        let Planned {
            output,
            deletes,
            writes,
        } = plan.plan(rows)?;
        self.placed(
            output,
            deletes,
            writes,
            "repository link has a mismatched placement",
        )
    }

    fn git(&self, rows: &mut PlanRows) -> EventStep {
        let DocumentEvent::Upsert {
            target,
            bytes,
            change,
            ..
        } = &self.event
        else {
            return Err(ApplyError::Invalid(
                "Git record handler received another event".to_string(),
            )
            .into());
        };
        let document_id = match target {
            DocumentTarget::GitPack {
                document_id,
                sha256,
            } => {
                let valid = postcard::from_bytes::<GitPackRecord>(bytes)
                    .is_ok_and(|pack| pack.document_id == *document_id && pack.valid(sha256));
                if !valid {
                    return self.reject("Git pack does not match its target or size limit");
                }
                // Equal hashes mean equal bytes; which record first named them does not matter.
                if rows
                    .read(target.storage_keyspace(), target.storage_key())?
                    .is_some()
                {
                    return Ok(Planned::output(ApplyOutcome::Skipped));
                }
                *document_id
            }
            DocumentTarget::GitRecord { .. } => {
                let Ok(record) = postcard::from_bytes::<GitRecord>(bytes) else {
                    // Topic history still holds records from before packs moved into Fjall; every
                    // holder then had them and `aruna-doctor migrate` converted its copy.
                    return Ok(Planned::output(ApplyOutcome::Skipped));
                };
                if *target
                    != (DocumentTarget::GitRecord {
                        document_id: record.document_id,
                        event_id: record.event_id,
                    })
                    || record.realm_id != self.realm_id
                    || !record.validate()
                {
                    return self.reject("Git record does not match its target, realm or bounds");
                }
                let registry = DocumentTarget::MetadataRegistry {
                    group_id: record.group_id,
                    document_id: record.document_id,
                };
                if let Some(value) =
                    rows.read(registry.storage_keyspace(), registry.storage_key())?
                {
                    let document: MetadataRegistryRecord =
                        postcard::from_bytes(&value).map_err(invalid)?;
                    if document.realm_id != record.realm_id
                        || document.placement != record.placement
                    {
                        return self.reject("Git record does not match its document");
                    }
                }
                match rows.read(target.storage_keyspace(), target.storage_key())? {
                    Some(stored) if stored.as_ref() == bytes.as_slice() => {
                        return Ok(Planned::output(ApplyOutcome::Skipped));
                    }
                    Some(_) => {
                        return self.reject("Git record id is already used by a different record");
                    }
                    None => record.document_id,
                }
            }
            _ => {
                return Err(ApplyError::Invalid(
                    "Git record handler received another event".to_string(),
                )
                .into());
            }
        };
        // The revision and manifest rows let this holder prove the shard in a handover.
        let mut writes = vec![(
            target.storage_keyspace().to_string(),
            target.storage_key(),
            bytes.clone().into(),
        )];
        writes.extend(revision_entries(target, change)?);
        Ok(Planned::changes(
            ApplyOutcome::Applied {
                target: target.clone(),
                tombstone: None,
                git_document: Some(document_id),
            },
            Vec::new(),
            writes,
        ))
    }

    /// Both vault records are immutable and signed by the node that admitted the user's own
    /// request; a realm node may relay them unchanged.
    fn vault(&self, rows: &mut PlanRows) -> EventStep {
        let DocumentEvent::Upsert {
            target,
            bytes,
            change,
            ..
        } = &self.event
        else {
            return self.reject("vault records are upserts");
        };
        let revision = match check_vault_record(target, bytes, change, self.realm_id) {
            Ok(revision) => revision,
            Err(reason) => return self.reject(reason),
        };
        if !published_by(self.topic_id, self.actor_id, &change.current.actor)
            && !relay_allowed(rows, self.topic_id, self.actor_id, self.realm_id)?
        {
            return self.reject("vault record publisher is neither its origin nor a realm relay");
        }
        let stored = rows.read(target.storage_keyspace(), target.storage_key())?;
        let applied = || ApplyOutcome::applied(target.clone());
        match (stored, revision) {
            (Some(stored), _) if stored.as_ref() == bytes.as_slice() => {
                Ok(Planned::output(ApplyOutcome::Skipped))
            }
            (Some(_), _) => self.reject("vault record id is already used by a different record"),
            (None, None) => {
                let writes = record_rows(target, bytes, change).map_err(invalid)?;
                Ok(Planned::changes(applied(), Vec::new(), writes))
            }
            (None, Some(revision)) => {
                let (mut writes, deletes) = head_rows(&revision, bytes).map_err(invalid)?;
                // A later save already retired this revision: it never becomes a head again.
                let marker = user_record_key(revision.user_id, revision.revision_id);
                let retired = rows.read(VAULT_RETIRED_KEYSPACE, marker.clone())?.is_some();
                if retired {
                    writes.retain(|(space, key, _)| {
                        space != VAULT_REVISION_KEYSPACE || key != &marker
                    });
                }
                let output = if retired {
                    ApplyOutcome::Skipped
                } else {
                    applied()
                };
                Ok(Planned::changes(output, deletes, writes))
            }
        }
    }

    /// Any other target: a malformed or unsupported event is quarantined.
    fn other(&self, rows: &mut PlanRows) -> EventStep {
        let target = self.event.target().clone();
        let planned = match &self.event {
            DocumentEvent::Upsert {
                target,
                bytes,
                change,
                ..
            } => upsert_changes(rows, self.realm_id, target, bytes, change),
            DocumentEvent::Delete { target, change, .. } => {
                delete_changes(rows, self.realm_id, target, change)
            }
            DocumentEvent::AdminOperation { .. } => Err(ApplyError::Invalid(
                "admin operations apply through their own plan".to_string(),
            )
            .into()),
        };
        match planned {
            Ok(Planned {
                deletes, writes, ..
            }) => Ok(Planned::changes(
                ApplyOutcome::applied(target),
                deletes,
                writes,
            )),
            Err(PlanStop::Failed(ApplyError::Invalid(reason))) => self.reject(reason),
            Err(stop) => Err(stop),
        }
    }
}

impl RowPlan for ApplyEventPlan {
    type Output = ApplyOutcome;
    type Error = ApplyError;

    fn plan(&self, rows: &mut PlanRows) -> EventStep {
        let event = &self.event;
        let target = event.target();
        let plain = matches!(
            event,
            DocumentEvent::Upsert { .. } | DocumentEvent::Delete { .. }
        );
        match target {
            DocumentTarget::WatchSubscription { .. }
            | DocumentTarget::EmailPreference { .. }
            | DocumentTarget::WebhookSubscription { .. }
            | DocumentTarget::OwnerSecret { .. }
                if plain =>
            {
                self.watch(rows)
            }
            DocumentTarget::NodeUsage { .. }
            | DocumentTarget::WatchInterest { .. }
            | DocumentTarget::NodeInfo { .. } => self.shared(rows),
            DocumentTarget::MetadataRegistry { .. } if upsert(event) => self.registry(rows),
            DocumentTarget::MetadataCreateEvent { .. } if upsert(event) => self.create_event(),
            DocumentTarget::MetadataDocumentLifecycle { .. } if upsert(event) => {
                self.lifecycle(rows)
            }
            DocumentTarget::MetadataGraphLifecycle { .. } if upsert(event) => self.graph(rows),
            DocumentTarget::RepositoryLink { .. } => self.link(rows),
            DocumentTarget::GitRecord { .. } | DocumentTarget::GitPack { .. } if upsert(event) => {
                self.git(rows)
            }
            DocumentTarget::VaultRevision { .. } | DocumentTarget::UserKey { .. } => {
                self.vault(rows)
            }
            DocumentTarget::PersistentIdMapping { .. } => self.mapping(rows),
            DocumentTarget::PlacementPolicy { .. } => self.policy(rows),
            // Whole-document admin sync is unsupported, but must not wedge replay.
            _ if plain && reduced_admin_target(target).is_some() => {
                self.reject("unsupported whole-document admin sync event")
            }
            _ => self.other(rows),
        }
    }

    fn transaction(&self) -> TransactionMode {
        event_mode(self.event.target())
    }
}

/// Applies one watch or setting change unless the stored revision wins.
#[derive(Clone, Debug, PartialEq)]
pub struct WatchChangePlan {
    pub target: DocumentTarget,
    pub bytes: Option<Vec<u8>>,
    pub change: DocumentChange,
}

impl RowPlan for WatchChangePlan {
    type Output = bool;
    type Error = ApplyError;

    fn plan(&self, rows: &mut PlanRows) -> Step<Planned<bool>> {
        let change = watch_rows(rows, &self.target, self.bytes.as_ref(), &self.change)?;
        Ok(match change {
            Some((deletes, writes)) => Planned::changes(true, deletes, writes),
            None => Planned::output(false),
        })
    }
}

/// Stores a placement policy document under this realm's publication authority.
#[derive(Clone, Debug, PartialEq)]
pub struct PolicyChangePlan {
    pub realm_id: RealmId,
    pub document: PlacementPolicyDocument,
    pub placement: PlacementRef,
}

impl RowPlan for PolicyChangePlan {
    type Output = PlacementOutcome<bool>;
    type Error = ApplyError;

    fn plan(&self, rows: &mut PlanRows) -> Step<Planned<PlacementOutcome<bool>>> {
        policy_change(rows, self.realm_id, &self.document, self.placement)
    }
}

/// Targets whose own family decides every event kind, so an admin event there is refused by that
/// family and never reaches the admin checks.
pub(super) fn family_target(target: &DocumentTarget) -> bool {
    matches!(
        target,
        DocumentTarget::NodeUsage { .. }
            | DocumentTarget::WatchInterest { .. }
            | DocumentTarget::NodeInfo { .. }
            | DocumentTarget::RepositoryLink { .. }
            | DocumentTarget::VaultRevision { .. }
            | DocumentTarget::UserKey { .. }
            | DocumentTarget::PersistentIdMapping { .. }
            | DocumentTarget::PlacementPolicy { .. }
    )
}

/// Documents written as they arrive need no transaction; every other family fences its write.
fn event_mode(target: &DocumentTarget) -> TransactionMode {
    match target {
        DocumentTarget::NodeUsage { .. }
        | DocumentTarget::WatchInterest { .. }
        | DocumentTarget::NodeInfo { .. }
        | DocumentTarget::GitRecord { .. }
        | DocumentTarget::GitPack { .. } => TransactionMode::None,
        _ => TransactionMode::Write,
    }
}

fn upsert(event: &DocumentEvent) -> bool {
    matches!(event, DocumentEvent::Upsert { .. })
}

/// Notification settings and owner secrets: immutable register writes.
fn register_target(target: &DocumentTarget) -> bool {
    SettingChannel::of_target(target).is_some()
        || matches!(target, DocumentTarget::OwnerSecret { .. })
}

/// The rows a change deletes and writes.
type RowChanges = (Vec<(String, ByteView)>, Vec<Row>);

/// The rows of a watch or setting change, or `None` when the stored revision wins.
fn watch_rows(
    rows: &mut PlanRows,
    target: &DocumentTarget,
    bytes: Option<&Vec<u8>>,
    change: &DocumentChange,
) -> Step<Option<RowChanges>> {
    let current = rows
        .read(SYNC_REVISION_KEYSPACE, sync_revision_key(target))?
        .map(|value| postcard::from_bytes::<DocumentChange>(value.as_ref()))
        .transpose()
        .map_err(invalid)?;
    if !watch_change_applies(current.as_ref().map(|local| local.kind), change.kind) {
        return Ok(None);
    }
    let mut writes =
        vec![crate::storage_entries::sync_revision_entry(target, change).map_err(invalid)?];
    // Only watch subscriptions feed the interest digest; notification settings share the path.
    if let DocumentTarget::WatchSubscription { owner, .. } = target {
        writes.push((
            WATCH_INTEREST_KEYSPACE.to_string(),
            ByteView::from(interest_dirty_key(owner.realm_id)),
            ByteView::from(change.current.event_id.to_bytes().to_vec()),
        ));
    }
    let row = (target.storage_keyspace().to_string(), target.storage_key());
    let deletes = match bytes {
        Some(bytes) => {
            writes.push((row.0, row.1, ByteView::from(bytes.clone())));
            Vec::new()
        }
        None => vec![row],
    };
    Ok(Some((deletes, writes)))
}

/// A publication counts only against this realm's current view: the original publisher must be
/// permitted and its authorizing user must still hold write on the named path.
fn policy_change(
    rows: &mut PlanRows,
    realm_id: RealmId,
    document: &PlacementPolicyDocument,
    placement: PlacementRef,
) -> Step<Planned<PlacementOutcome<bool>>> {
    let deferred = |dependency| Ok(Planned::output(PlacementOutcome::Deferred(dependency)));
    let Some(config) = realm_config(rows, realm_id)? else {
        return deferred(DocumentSyncDependency::RealmConfig(realm_id));
    };
    let Some(auth) = realm_authorization(rows, realm_id)? else {
        return deferred(DocumentSyncDependency::RealmAuthorization(realm_id));
    };
    // A group-owned rule is decided against its owner's roles; without them nothing is accepted,
    // so the document waits instead of being trusted.
    let group_auth = match document.policy.owner_group_id {
        Some(group_id) => match group_authorization(rows, group_id)? {
            Some(group_auth) => Some(group_auth),
            None => return deferred(DocumentSyncDependency::GroupAuthorization(group_id)),
        },
        None => None,
    };
    if verify_policy_authority(document, &config, &auth, group_auth.as_ref()).is_err() {
        return Ok(Planned::output(PlacementOutcome::Rejected));
    }
    PolicyDocumentPlan {
        realm_id,
        incoming: document.clone(),
        placement,
    }
    .plan(rows)
}

/// The checks and the row of a document stored as it arrives.
fn plain_upsert(rows: &mut PlanRows, target: &DocumentTarget, bytes: &[u8]) -> Step<Vec<Row>> {
    match target {
        // Structural guards for the shared keyspaces: re-check self-consistency before the write.
        DocumentTarget::NodeUsage { .. } => {
            validate_usage_upsert(target, bytes).map_err(ApplyError::Invalid)?;
        }
        DocumentTarget::WatchInterest { .. } => {
            validate_watch_interest(target, bytes).map_err(ApplyError::Invalid)?;
        }
        DocumentTarget::NodeInfo { .. } => {
            let incoming = validate_node_upsert(target, bytes).map_err(ApplyError::Invalid)?;
            let stored = rows.read(target.storage_keyspace(), target.storage_key())?;
            if !node_info_supersedes(&incoming, stored.as_ref().map(|value| value.as_ref())) {
                return Ok(Vec::new());
            }
        }
        _ => {}
    }
    Ok(vec![(
        target.storage_keyspace().to_string(),
        target.storage_key(),
        bytes.to_vec().into(),
    )])
}

fn unit<T>(planned: Planned<T>) -> Planned<()> {
    Planned::changes((), planned.deletes, planned.writes)
}

/// The rows of an upsert applied outside the reconcile loop's per-family handling.
fn upsert_changes(
    rows: &mut PlanRows,
    realm_id: RealmId,
    target: &DocumentTarget,
    bytes: &[u8],
    change: &DocumentChange,
) -> Step<Planned<()>> {
    let wrong = |reason: String| ApplyError::Invalid(reason);
    if reduced_admin_target(target).is_some() {
        return Err(wrong(
            "whole-document admin sync is unsupported; admin documents must sync as operations"
                .to_string(),
        )
        .into());
    }
    match target {
        DocumentTarget::MetadataCreateEvent {
            document_id,
            event_id,
        } => {
            let record: MetadataEventRecord = postcard::from_bytes(bytes).map_err(invalid)?;
            if record.record.document_id != *document_id || record.event_id != *event_id {
                return Err(wrong(format!(
                    "replicated metadata create-event target {document_id}/{event_id} does not match payload {}/{}",
                    record.record.document_id, record.event_id
                ))
                .into());
            }
            validate_metadata_event(&record)?;
            let plan = CreateEventPlan {
                event: record,
                target: target.clone(),
                bytes: bytes.to_vec(),
            };
            plan.plan(rows)
        }
        DocumentTarget::GitRecord { .. } | DocumentTarget::GitPack { .. } => {
            // Git records only arrive through the reconcile loop, which validates them.
            Err(wrong("Git records must be applied through document reconcile".to_string()).into())
        }
        DocumentTarget::VaultRevision { .. } | DocumentTarget::UserKey { .. } => {
            // Vault records check their author, which only the reconcile loop knows.
            Err(
                wrong("vault records must be applied through document reconcile".to_string())
                    .into(),
            )
        }
        DocumentTarget::MetadataDocumentLifecycle { document_id } => {
            let record: MetadataLifecycleRecord = postcard::from_bytes(bytes).map_err(invalid)?;
            if record.document_id() != *document_id {
                return Err(wrong(format!(
                    "replicated metadata document lifecycle target {document_id} does not match payload document {}",
                    record.document_id()
                ))
                .into());
            }
            let plan = DocumentLifecyclePlan {
                record,
                change: *change,
            };
            Ok(unit(plan.plan(rows)?))
        }
        DocumentTarget::MetadataRegistry {
            group_id,
            document_id,
        } => {
            let record: MetadataRegistryRecord = postcard::from_bytes(bytes).map_err(invalid)?;
            if record.group_id != *group_id || record.document_id != *document_id {
                return Err(wrong(format!(
                    "replicated metadata registry target {group_id}/{document_id} does not match payload {}/{}",
                    record.group_id, record.document_id
                ))
                .into());
            }
            let plan = RegistryUpsertPlan {
                record,
                primary_bytes: bytes.to_vec(),
            };
            Ok(unit(plan.plan(rows)?))
        }
        DocumentTarget::PersistentIdMapping { document_id } => {
            let mapping = PersistentIdMapping::from_bytes(bytes).map_err(invalid)?;
            validate_pid_mapping(*document_id, &mapping, change).map_err(ApplyError::Invalid)?;
            // The generic write would clobber a local tombstone with a replayed Active row, so
            // the mapping always goes through its merge.
            let plan = PidMappingPlan {
                realm_id,
                incoming: mapping,
                placement: change.placement,
            };
            let planned = plan.plan(rows)?;
            match planned.output {
                PlacementOutcome::Accepted(_) => Ok(unit(planned)),
                PlacementOutcome::Deferred(_) => Err(ApplyError::Failed(
                    "persistent id mapping placement configuration is unavailable".to_string(),
                )
                .into()),
                PlacementOutcome::Rejected => Err(wrong(
                    "persistent id mapping has a mismatched placement configuration".to_string(),
                )
                .into()),
            }
        }
        DocumentTarget::PlacementPolicy { policy_id } => {
            let document: PlacementPolicyDocument = postcard::from_bytes(bytes).map_err(invalid)?;
            validate_policy_document(*policy_id, realm_id, &document, change)
                .map_err(ApplyError::Invalid)?;
            // The generic write would replace a stored rule with different bytes under the same
            // id, so a policy always goes through its merge.
            let planned = policy_change(rows, realm_id, &document, change.placement)?;
            match planned.output {
                PlacementOutcome::Accepted(_) => Ok(unit(planned)),
                // The live loop handles this target and registers the dependency; failing here
                // keeps the cursor and redelivers the event.
                PlacementOutcome::Deferred(_) => Err(ApplyError::Deferred(format!(
                    "placement policy {policy_id} awaits its authority evidence"
                ))
                .into()),
                PlacementOutcome::Rejected => Err(wrong(
                    "placement policy has a mismatched placement or reuses its id".to_string(),
                )
                .into()),
            }
        }
        DocumentTarget::RepositoryLink { .. } => {
            validate_link(target, Some(bytes), change).map_err(ApplyError::Invalid)?;
            link_changes(rows, realm_id, target, Some(bytes.to_vec()), change)
        }
        DocumentTarget::MetadataGraphLifecycle { graph_iri } => {
            let record: GraphLifecycleRecord = postcard::from_bytes(bytes).map_err(invalid)?;
            if record.graph_iri != *graph_iri {
                return Err(wrong(format!(
                    "replicated metadata graph lifecycle target `{graph_iri}` does not match payload graph `{}`",
                    record.graph_iri
                ))
                .into());
            }
            let plan = GraphLifecyclePlan {
                record,
                primary_bytes: bytes.to_vec(),
            };
            Ok(unit(plan.plan(rows)?))
        }
        _ => {
            let writes = plain_upsert(rows, target, bytes)?;
            Ok(Planned::changes((), Vec::new(), writes))
        }
    }
}

fn link_changes(
    rows: &mut PlanRows,
    realm_id: RealmId,
    target: &DocumentTarget,
    bytes: Option<Vec<u8>>,
    change: &DocumentChange,
) -> Step<Planned<()>> {
    let plan = LinkPlan {
        realm_id,
        target: target.clone(),
        bytes,
        change: *change,
    };
    let planned = plan.plan(rows)?;
    match planned.output {
        PlacementOutcome::Accepted(_) => Ok(unit(planned)),
        PlacementOutcome::Deferred(_) => Err(ApplyError::Failed(
            "repository link placement configuration is unavailable".to_string(),
        )
        .into()),
        PlacementOutcome::Rejected => Err(ApplyError::Invalid(
            "repository link has a mismatched placement".to_string(),
        )
        .into()),
    }
}

fn delete_changes(
    rows: &mut PlanRows,
    realm_id: RealmId,
    target: &DocumentTarget,
    change: &DocumentChange,
) -> Step<Planned<()>> {
    if change.kind != DocumentChangeKind::Delete {
        return Err(ApplyError::Invalid(
            "document sync delete must carry a delete change".to_string(),
        )
        .into());
    }
    match target {
        // Lifecycle rows, immutable Git history and PIDs are never removed by a delete: a minted
        // PID only flips to Withdrawn, checkpoints supersede Git records.
        DocumentTarget::MetadataGraphLifecycle { .. }
        | DocumentTarget::MetadataDocumentLifecycle { .. }
        | DocumentTarget::GitRecord { .. }
        | DocumentTarget::GitPack { .. }
        | DocumentTarget::PersistentIdMapping { .. } => Ok(Planned::output(())),
        DocumentTarget::RepositoryLink { .. } => {
            validate_link(target, None, change).map_err(ApplyError::Invalid)?;
            link_changes(rows, realm_id, target, None, change)
        }
        DocumentTarget::MetadataRegistry {
            group_id,
            document_id,
        } => DeleteRegistryPlan {
            group_id: *group_id,
            document_id: *document_id,
        }
        .plan(rows),
        _ if reduced_admin_target(target).is_some() => Err(ApplyError::Invalid(
            "whole-document admin sync is unsupported; admin documents must sync as operations"
                .to_string(),
        )
        .into()),
        _ => Err(
            ApplyError::Invalid("document sync delete target is unsupported".to_string()).into(),
        ),
    }
}
