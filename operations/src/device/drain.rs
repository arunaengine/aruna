//! Publishes queued authoring intents once the realm is reachable again.

use std::sync::Arc;
use std::time::Duration;

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::DEVICE_INTAKE_KEYSPACE;
use aruna_core::metadata::{MetadataAuthToken, MetadataError};
use aruna_core::structs::{Actor, AuthContext, RealmConfigDocument, RealmId};
use aruna_core::structured_id::StructuredId;
use aruna_core::task::TaskKey;
use aruna_core::time::unix_timestamp_millis;
use aruna_core::types::{Key, TxnId};
use aruna_storage::storage::StorageHandle;
use aruna_tasks::TaskHandle;
use tracing::{info, warn};
use ulid::Ulid;

use crate::driver::DriverContext;
use crate::metadata::create_document::{
    CreateMetadataDocumentConfig, CreateMetadataDocumentError, CreateMetadataDocumentOperation,
    CreateMetadataDocumentPayload, mint_forward_document,
};
use crate::metadata::forward::{MetadataWriteError, apply_batch_routed, route_metadata_create};
use crate::metadata::update_document::UpdateMetadataDocumentError;
use crate::placement::process_placements::load_realm_config;

use super::backlog::{ForwardOutcome, QueueDrain, arm_timer, drain_queue, exhausted, retry_due_ms};
use super::publish_queue::{
    MAX_PUBLISH_ATTEMPTS, PublishEntry, PublishKind, PublishState, entry_with_state, publish_entry,
    read_publish_entry, scan_publish_queue,
};
use super::replica::{read_replica, store_replica};
use super::selection::track_created;

/// Delay before a deferred pass looks for the realm again.
pub const PUBLISH_DEFER_RETRY_AFTER: Duration = Duration::from_secs(15);

/// Delay between passes while entries are still due.
pub const PUBLISH_CONTINUE_AFTER: Duration = Duration::from_millis(250);

/// What one drain pass achieved, so the caller knows how soon to look again.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DrainOutcome {
    /// The pass could not complete; earlier pages may already be forwarded.
    Deferred,
    /// At least one entry asked for another pass; it does not prove entries remain.
    Recheck,
    /// No entry asked for another pass.
    Idle,
}

/// Forwards every due entry of the whole queue, oldest first. The scan pages
/// past entries it skips, so published and parked ones cannot starve the tail.
pub async fn drain_publish_queue(context: &Arc<DriverContext>) -> DrainOutcome {
    let Some(net_handle) = context.net_handle.as_ref() else {
        return DrainOutcome::Deferred;
    };
    let realm_id = *net_handle.realm_id();
    let node_id = net_handle.node_id();
    let Some(config) = load_realm_config(context, realm_id).await else {
        return DrainOutcome::Deferred;
    };

    let now = unix_timestamp_millis();
    drain_queue(
        now,
        PublishDrain {
            context,
            config: &config,
            realm_id,
            node_id,
        },
    )
    .await
}

/// One publish drain queue handed to the shared page loop.
struct PublishDrain<'a> {
    context: &'a Arc<DriverContext>,
    config: &'a RealmConfigDocument,
    realm_id: RealmId,
    node_id: aruna_core::NodeId,
}

impl QueueDrain for PublishDrain<'_> {
    type Entry = PublishEntry;

    async fn read(&mut self, cursor: Option<Key>) -> Option<(Vec<PublishEntry>, Option<Key>)> {
        read_page(self.context, cursor).await
    }

    async fn forward(&mut self, entry: PublishEntry) -> ForwardOutcome {
        let Some(claim) = claim_entry(
            self.context,
            self.config,
            self.realm_id,
            self.node_id,
            &entry,
        )
        .await
        else {
            // A failed claim still asks for another pass: the entry may have
            // advanced rather than the queue being empty.
            return ForwardOutcome::Recheck;
        };
        let next = forward_entry(self.context, self.realm_id, self.node_id, &entry, &claim).await;
        store_entry(self.context, &entry_with_state(&entry, next)).await;
        ForwardOutcome::Recheck
    }
}

/// One page of the queue and the cursor of the next one. `None` means the scan
/// itself failed.
async fn read_page(
    context: &Arc<DriverContext>,
    cursor: Option<Key>,
) -> Option<(Vec<PublishEntry>, Option<Key>)> {
    let Effect::Storage(effect) = scan_publish_queue(cursor, None) else {
        return None;
    };
    match context.storage_handle.send_storage_effect(effect).await {
        Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) => Some((
            values
                .into_iter()
                .filter_map(|(_, bytes)| PublishEntry::from_bytes(&bytes).ok())
                .collect(),
            next_start_after,
        )),
        Event::Storage(StorageEvent::Error { error }) => {
            warn!(error = %error, "Failed to scan the device publish queue");
            None
        }
        other => {
            warn!(event = ?other, "Unexpected event while scanning the device publish queue");
            None
        }
    }
}

/// Stores `next` only while the entry still carries the state the scan read.
/// A delete committed in between therefore wins instead of the entry coming
/// back as `Publishing`.
pub(super) async fn claim_state(
    context: &Arc<DriverContext>,
    entry: &PublishEntry,
    next: PublishState,
) -> bool {
    let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = context
        .storage_handle
        .send_storage_effect(StorageEffect::StartTransaction { read: false })
        .await
    else {
        warn!(draft_id = %entry.draft_id, "Failed to open a transaction for a queued draft");
        return false;
    };
    if !write_claim(context, entry, next, txn_id).await {
        context
            .storage_handle
            .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
            .await;
        return false;
    }
    matches!(
        context
            .storage_handle
            .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
            .await,
        Event::Storage(StorageEvent::TransactionCommitted { .. })
    )
}

async fn write_claim(
    context: &Arc<DriverContext>,
    entry: &PublishEntry,
    next: PublishState,
    txn_id: TxnId,
) -> bool {
    let Effect::Storage(read) = read_publish_entry(entry.draft_id, Some(txn_id)) else {
        return false;
    };
    let current = match context.storage_handle.send_storage_effect(read).await {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(bytes), ..
        }) => PublishEntry::from_bytes(&bytes).ok(),
        _ => None,
    };
    if !current.is_some_and(|current| current.state == entry.state) {
        return false;
    }
    let Ok((key_space, key, value)) = publish_entry(&entry_with_state(entry, next)) else {
        warn!(draft_id = %entry.draft_id, "Failed to encode a queued draft");
        return false;
    };
    matches!(
        context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space,
                key,
                value,
                txn_id: Some(txn_id),
            })
            .await,
        Event::Storage(StorageEvent::WriteResult { .. })
    )
}

async fn store_entry(context: &Arc<DriverContext>, entry: &PublishEntry) {
    let Ok((key_space, key, value)) = publish_entry(entry) else {
        warn!(draft_id = %entry.draft_id, "Failed to encode a queued draft");
        return;
    };
    if let Event::Storage(StorageEvent::Error { error }) = context
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space,
            key,
            value,
            txn_id: None,
        })
        .await
    {
        warn!(error = %error, draft_id = %entry.draft_id, "Failed to store a queued draft");
    }
}

/// What one pass holds on an entry it may forward.
struct Claim {
    document_id: Ulid,
    attempts: u32,
}

/// Mints the entry's realm document id when it has none and claims it as
/// `Publishing`. `None` means this pass must leave the entry alone: the mint
/// failed, or the entry was deleted or advanced while the page was in flight.
async fn claim_entry(
    context: &Arc<DriverContext>,
    config: &RealmConfigDocument,
    realm_id: RealmId,
    node_id: aruna_core::NodeId,
    entry: &PublishEntry,
) -> Option<Claim> {
    let attempts = entry.attempts().saturating_add(1);
    let document_id = match (&entry.kind, &entry.state) {
        // An edit already names its document; nothing is minted for it.
        (PublishKind::Edit { document_id, .. }, _) => *document_id,
        (_, PublishState::Publishing { document_id, .. }) => *document_id,
        _ => {
            let actor = Actor {
                node_id,
                user_id: entry.owner,
                realm_id,
            };
            match mint_forward_document(config, &actor, entry.group_id, &entry.document_path) {
                Ok(minted) => minted.as_ulid(),
                // Placement or configuration is not settled yet; try again later.
                Err(error) => {
                    claim_state(context, entry, retry_state(attempts, error.to_string())).await;
                    return None;
                }
            }
        }
    };

    // The minted id is stored before the forward, so a crash re-forwards the
    // same id and the holder's create fence dedups it; an uncommitted claim stops it.
    claim_state(
        context,
        entry,
        PublishState::Publishing {
            document_id,
            due_at_ms: unix_timestamp_millis(),
            attempts,
        },
    )
    .await
    .then_some(Claim {
        document_id,
        attempts,
    })
}

/// Forwards one claimed entry and answers with the state it must be stored
/// under. The entry is `Publishing` by then, so the owner cannot delete it
/// underneath the forward.
async fn forward_entry(
    context: &Arc<DriverContext>,
    realm_id: RealmId,
    node_id: aruna_core::NodeId,
    entry: &PublishEntry,
    claim: &Claim,
) -> PublishState {
    let actor = Actor {
        node_id,
        user_id: entry.owner,
        realm_id,
    };
    let Claim {
        document_id,
        attempts,
    } = *claim;
    let auth = AuthContext {
        user_id: entry.owner,
        realm_id,
        path_restrictions: None,
        session: None,
    };
    if matches!(entry.kind, PublishKind::Edit { .. }) {
        return publish_edit(context, realm_id, entry, claim, auth).await;
    }
    let operation =
        CreateMetadataDocumentOperation::new_generated_id(CreateMetadataDocumentConfig {
            actor,
            group_id: entry.group_id,
            document_id,
            document_path: entry.document_path.clone(),
            public: entry.public,
            payload: CreateMetadataDocumentPayload::RoCrate {
                jsonld: entry.jsonld.clone(),
            },
        });
    match route_metadata_create(
        operation,
        context.clone(),
        Some(MetadataAuthToken::internal(auth)),
    )
    .await
    {
        Ok(_) => {
            info!(draft_id = %entry.draft_id, document_id = %document_id, "Published a queued draft");
            track_created(
                context,
                document_id,
                entry.group_id,
                entry.document_path.clone(),
            )
            .await;
            PublishState::Published { document_id }
        }
        // The id was minted for this entry alone, so an existing document under
        // it is this entry's own earlier forward.
        Err(MetadataWriteError::Create(CreateMetadataDocumentError::DocumentAlreadyExists)) => {
            track_created(
                context,
                document_id,
                entry.group_id,
                entry.document_path.clone(),
            )
            .await;
            PublishState::Published { document_id }
        }
        Err(error) if permanent(&error) => PublishState::Failed {
            reason: error.to_string(),
            retryable: false,
            document_id: None,
        },
        // An unknown outcome keeps the minted id, so the retry is the same
        // create rather than a second document.
        Err(error) => publishing_retry(document_id, attempts, error.to_string()),
    }
}

/// Forwards one claimed edit. The batch is what the owner already applied
/// locally, so a holder that accepts it converges with this device.
async fn publish_edit(
    context: &Arc<DriverContext>,
    realm_id: RealmId,
    entry: &PublishEntry,
    claim: &Claim,
    auth: AuthContext,
) -> PublishState {
    let PublishKind::Edit {
        document_id,
        batch,
        authored,
    } = &entry.kind
    else {
        return PublishState::Failed {
            reason: "not an edit".to_string(),
            retryable: false,
            document_id: None,
        };
    };
    match apply_batch_routed(
        context,
        realm_id,
        *document_id,
        batch.clone(),
        authored.clone(),
        MetadataAuthToken::internal(auth),
    )
    .await
    {
        Ok(_) => {
            info!(draft_id = %entry.draft_id, document_id = %document_id, "Published an offline edit");
            settle_edit(context, *document_id).await;
            PublishState::Published {
                document_id: *document_id,
            }
        }
        Err(error) if permanent(&error) => PublishState::Failed {
            reason: error.to_string(),
            retryable: false,
            document_id: Some(*document_id),
        },
        Err(error) => publishing_retry(*document_id, claim.attempts, error.to_string()),
    }
}

/// Records that the realm has seen one of this replica's edits.
async fn settle_edit(context: &Arc<DriverContext>, document_id: Ulid) {
    let Some(mut replica) = read_replica(context, document_id).await else {
        return;
    };
    replica.pending_edits = replica.pending_edits.saturating_sub(1);
    store_replica(context, &replica).await;
}

/// Authorization, target and payload verdicts do not improve by waiting.
fn permanent(error: &MetadataWriteError) -> bool {
    matches!(
        error,
        MetadataWriteError::Unauthorized
            | MetadataWriteError::Forbidden
            | MetadataWriteError::NotFound
            | MetadataWriteError::Update(UpdateMetadataDocumentError::MetadataError(
                MetadataError::InvalidInput(_)
            ))
    )
}

/// Backoff before the entry is minted: nothing has been forwarded yet.
fn retry_state(attempts: u32, reason: String) -> PublishState {
    if exhausted(attempts, MAX_PUBLISH_ATTEMPTS) {
        return PublishState::Failed {
            reason,
            retryable: true,
            document_id: None,
        };
    }
    PublishState::Pending {
        due_at_ms: retry_due_ms(attempts),
        attempts,
        last_error: Some(reason),
    }
}

/// Backoff after a forward whose outcome is unknown. The minted id is kept so
/// the next attempt is the same create rather than a second document.
fn publishing_retry(document_id: Ulid, attempts: u32, reason: String) -> PublishState {
    if exhausted(attempts, MAX_PUBLISH_ATTEMPTS) {
        return PublishState::Failed {
            reason: format!("{reason} (document id {document_id})"),
            retryable: true,
            document_id: Some(document_id),
        };
    }
    PublishState::Publishing {
        document_id,
        due_at_ms: retry_due_ms(attempts),
        attempts,
    }
}

/// Re-arms the drain when the queue still holds entries.
pub async fn restore_publish_timer(storage: &StorageHandle, task_handle: &TaskHandle) {
    let event = storage
        .send_storage_effect(StorageEffect::Iter {
            key_space: DEVICE_INTAKE_KEYSPACE.to_string(),
            prefix: None,
            start: None,
            limit: 1,
            txn_id: None,
        })
        .await;
    let has_entries = match event {
        Event::Storage(StorageEvent::IterResult { values, .. }) => !values.is_empty(),
        Event::Storage(StorageEvent::Error { error }) => {
            warn!(error = %error, "Failed to scan the device publish queue");
            return;
        }
        other => {
            warn!(event = ?other, "Unexpected event while scanning the device publish queue");
            return;
        }
    };
    if has_entries {
        arm_timer(
            task_handle,
            TaskKey::DrainDeviceIntake,
            "Failed to restore the device publish timer",
        )
        .await;
    }
}

#[cfg(test)]
mod tests {
    use super::{
        DrainOutcome, claim_state, drain_publish_queue, permanent, publishing_retry, retry_state,
    };
    use crate::device::delete_draft::DeleteDraftOperation;
    use crate::device::inspect_draft::{InspectDraftError, InspectDraftOperation};
    use crate::device::publish_queue::{
        MAX_PUBLISH_ATTEMPTS, PublishEntry, PublishState, publish_entry,
    };
    use crate::device::tests::fixtures::context;
    use crate::driver::{DriverContext, drive};
    use crate::metadata::forward::MetadataWriteError;
    use aruna_core::effects::StorageEffect;
    use aruna_core::structs::RealmId;
    use aruna_core::types::UserId;
    use std::sync::Arc;
    use ulid::Ulid;

    fn entry() -> PublishEntry {
        PublishEntry::new(
            Ulid::generate(),
            UserId::local(Ulid::generate(), RealmId::from_bytes([6u8; 32])),
            Ulid::generate(),
            "/notes".to_string(),
            false,
            "{}".to_string(),
        )
    }

    async fn store(context: &Arc<DriverContext>, entry: &PublishEntry) {
        let (key_space, key, value) = publish_entry(entry).unwrap();
        context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space,
                key,
                value,
                txn_id: None,
            })
            .await;
    }

    fn publishing() -> PublishState {
        PublishState::Publishing {
            document_id: Ulid::from_bytes([4u8; 16]),
            due_at_ms: 1,
            attempts: 1,
        }
    }

    #[tokio::test]
    async fn defers_without_realm() {
        // No net handle means the realm is unreachable: nothing may be touched.
        let (_tempdir, context) = context().await;
        let context = Arc::new(context);
        let entry = entry();
        store(&context, &entry).await;
        assert_eq!(drain_publish_queue(&context).await, DrainOutcome::Deferred);
    }

    #[test]
    fn keeps_minted_id() {
        // A forward with an unknown outcome must never mint a second id.
        let document_id = Ulid::generate();
        let PublishState::Publishing {
            document_id: kept,
            due_at_ms,
            attempts,
        } = publishing_retry(document_id, 1, "unreachable".to_string())
        else {
            panic!("a retryable forward stays in publishing");
        };
        assert_eq!(kept, document_id);
        assert_eq!(attempts, 1);
        assert!(due_at_ms > 0);
    }

    #[test]
    fn parks_exhausted_entries() {
        let document_id = Ulid::generate();
        assert!(matches!(
            publishing_retry(document_id, MAX_PUBLISH_ATTEMPTS, "unreachable".to_string()),
            PublishState::Failed {
                retryable: true,
                document_id: Some(kept),
                ..
            } if kept == document_id
        ));
        assert!(matches!(
            retry_state(MAX_PUBLISH_ATTEMPTS, "no placement".to_string()),
            PublishState::Failed {
                retryable: true,
                document_id: None,
                ..
            }
        ));
    }

    #[test]
    fn backs_off_unminted() {
        let PublishState::Pending {
            attempts,
            last_error,
            ..
        } = retry_state(2, "no placement".to_string())
        else {
            panic!("an unminted entry stays pending");
        };
        assert_eq!(attempts, 2);
        assert_eq!(last_error.as_deref(), Some("no placement"));
    }

    #[tokio::test]
    async fn skips_deleted_entry() {
        // The owner's delete committed while the page was in flight: the entry
        // must stay gone instead of coming back as publishing.
        let (_tempdir, context) = context().await;
        let context = Arc::new(context);
        let entry = entry();
        store(&context, &entry).await;
        drive(DeleteDraftOperation::new(entry.draft_id), context.as_ref())
            .await
            .unwrap();

        assert!(!claim_state(&context, &entry, publishing()).await);
        assert_eq!(
            drive(InspectDraftOperation::new(entry.draft_id), context.as_ref()).await,
            Err(InspectDraftError::NotFound)
        );
    }

    #[tokio::test]
    async fn skips_advanced_entry() {
        let (_tempdir, context) = context().await;
        let context = Arc::new(context);
        let entry = entry();
        let advanced = PublishEntry {
            state: PublishState::Failed {
                reason: "parked".to_string(),
                retryable: true,
                document_id: None,
            },
            ..entry.clone()
        };
        store(&context, &advanced).await;

        assert!(!claim_state(&context, &entry, publishing()).await);
        assert_eq!(
            drive(InspectDraftOperation::new(entry.draft_id), context.as_ref())
                .await
                .unwrap()
                .state,
            advanced.state
        );
    }

    #[tokio::test]
    async fn claims_unchanged_entry() {
        let (_tempdir, context) = context().await;
        let context = Arc::new(context);
        let entry = entry();
        store(&context, &entry).await;

        assert!(claim_state(&context, &entry, publishing()).await);
        assert_eq!(
            drive(InspectDraftOperation::new(entry.draft_id), context.as_ref())
                .await
                .unwrap()
                .state,
            publishing()
        );
    }

    #[test]
    fn classifies_permanent_errors() {
        assert!(permanent(&MetadataWriteError::Forbidden));
        assert!(!permanent(&MetadataWriteError::Undeliverable(
            "no holder".to_string()
        )));
    }
}
