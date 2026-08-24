//! Publishes queued authoring intents once the realm is reachable again.

use std::sync::Arc;
use std::time::Duration;

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::DEVICE_INTAKE_KEYSPACE;
use aruna_core::metadata::MetadataAuthToken;
use aruna_core::structs::{Actor, AuthContext, RealmConfigDocument, RealmId};
use aruna_core::structured_id::StructuredId;
use aruna_core::task::{TaskEvent, TaskKey};
use aruna_core::types::{Key, TxnId};
use aruna_core::util::unix_timestamp_millis;
use aruna_storage::storage::StorageHandle;
use aruna_tasks::TaskHandle;
use tracing::{info, warn};
use ulid::Ulid;

use crate::create_metadata_document::{
    CreateMetadataDocumentConfig, CreateMetadataDocumentError, CreateMetadataDocumentOperation,
    CreateMetadataDocumentPayload, mint_forward_document,
};
use crate::driver::DriverContext;
use crate::metadata::forward::{MetadataWriteError, create_metadata_document_routed};
use crate::process_placements::load_realm_config;
use crate::queue_backoff::queue_retry_after_ms;

use super::repository::{
    IntakeEntry, IntakeState, MAX_INTAKE_ATTEMPTS, entry_with_state, intake_entry, read_intake,
    scan_intake,
};

/// Delay before a deferred pass looks for the realm again.
pub const INTAKE_DEFER_RETRY_AFTER: Duration = Duration::from_secs(15);

/// Delay between passes while entries are still due.
pub const INTAKE_CONTINUE_AFTER: Duration = Duration::from_millis(250);

/// What one drain pass achieved, so the caller knows how soon to look again.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DrainOutcome {
    /// The realm is not usable yet and nothing was forwarded.
    Deferred,
    /// Entries are still due after this pass.
    More,
    /// No entry is due.
    Idle,
}

/// Forwards every due entry of the whole queue, oldest first. The scan pages
/// past entries it skips, so published and parked ones cannot starve the tail.
pub async fn drain_intake(context: &Arc<DriverContext>) -> DrainOutcome {
    let Some(net_handle) = context.net_handle.as_ref() else {
        return DrainOutcome::Deferred;
    };
    let realm_id = *net_handle.realm_id();
    let node_id = net_handle.node_id();
    let Some(config) = load_realm_config(context, realm_id).await else {
        return DrainOutcome::Deferred;
    };

    let now = unix_timestamp_millis();
    let mut cursor = None;
    let mut due = false;
    loop {
        let Some((entries, next_cursor)) = read_page(context, cursor).await else {
            return DrainOutcome::Deferred;
        };
        for entry in entries {
            if !entry.is_due(now) {
                continue;
            }
            due = true;
            let Some(claim) = claim_entry(context, &config, realm_id, node_id, &entry).await else {
                continue;
            };
            let next = publish_entry(context, realm_id, node_id, &entry, &claim).await;
            store_entry(context, &entry_with_state(&entry, next)).await;
        }
        match next_cursor {
            Some(next_cursor) => cursor = Some(next_cursor),
            None => break,
        }
    }
    if due {
        DrainOutcome::More
    } else {
        DrainOutcome::Idle
    }
}

/// One page of the queue and the cursor of the next one. `None` means the scan
/// itself failed.
async fn read_page(
    context: &Arc<DriverContext>,
    cursor: Option<Key>,
) -> Option<(Vec<IntakeEntry>, Option<Key>)> {
    let Effect::Storage(effect) = scan_intake(cursor, None) else {
        return None;
    };
    match context.storage_handle.send_storage_effect(effect).await {
        Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) => Some((
            values
                .into_iter()
                .filter_map(|(_, bytes)| IntakeEntry::from_bytes(&bytes).ok())
                .collect(),
            next_start_after,
        )),
        Event::Storage(StorageEvent::Error { error }) => {
            warn!(error = %error, "Failed to scan the device authoring intake");
            None
        }
        other => {
            warn!(event = ?other, "Unexpected event while scanning the device authoring intake");
            None
        }
    }
}

/// Stores `next` only while the entry still carries the state the scan read.
/// A delete committed in between therefore wins instead of the entry coming
/// back as `Publishing`.
async fn claim_state(context: &Arc<DriverContext>, entry: &IntakeEntry, next: IntakeState) -> bool {
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
    entry: &IntakeEntry,
    next: IntakeState,
    txn_id: TxnId,
) -> bool {
    let Effect::Storage(read) = read_intake(entry.draft_id, Some(txn_id)) else {
        return false;
    };
    let current = match context.storage_handle.send_storage_effect(read).await {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(bytes), ..
        }) => IntakeEntry::from_bytes(&bytes).ok(),
        _ => None,
    };
    if !current.is_some_and(|current| current.state == entry.state) {
        return false;
    }
    let Ok((key_space, key, value)) = intake_entry(&entry_with_state(entry, next)) else {
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

async fn store_entry(context: &Arc<DriverContext>, entry: &IntakeEntry) {
    let Ok((key_space, key, value)) = intake_entry(entry) else {
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
    entry: &IntakeEntry,
) -> Option<Claim> {
    let attempts = entry.attempts().saturating_add(1);
    let document_id = match &entry.state {
        IntakeState::Publishing { document_id, .. } => *document_id,
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

    // The minted id is stored before the forward, so a crash mid-publish
    // re-forwards the same id and the holder answers from its create fence. A
    // claim that does not commit stops the attempt: forwarding without it would
    // mint a second id after a crash, and would resurrect a deleted entry.
    claim_state(
        context,
        entry,
        IntakeState::Publishing {
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
async fn publish_entry(
    context: &Arc<DriverContext>,
    realm_id: RealmId,
    node_id: aruna_core::NodeId,
    entry: &IntakeEntry,
    claim: &Claim,
) -> IntakeState {
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
    };
    let operation = CreateMetadataDocumentOperation::new_for_generated_document_id(
        CreateMetadataDocumentConfig {
            actor,
            group_id: entry.group_id,
            document_id,
            document_path: entry.document_path.clone(),
            public: entry.public,
            payload: CreateMetadataDocumentPayload::RoCrate {
                jsonld: entry.jsonld.clone(),
            },
        },
    );
    match create_metadata_document_routed(
        operation,
        context.clone(),
        Some(MetadataAuthToken::internal(auth)),
    )
    .await
    {
        Ok(_) => {
            info!(draft_id = %entry.draft_id, document_id = %document_id, "Published a queued draft");
            IntakeState::Published { document_id }
        }
        // The id was minted for this entry alone, so an existing document under
        // it is this entry's own earlier forward.
        Err(MetadataWriteError::Create(CreateMetadataDocumentError::DocumentAlreadyExists)) => {
            IntakeState::Published { document_id }
        }
        Err(error) if permanent(&error) => IntakeState::Failed {
            reason: error.to_string(),
            retryable: false,
        },
        // An unknown outcome keeps the minted id, so the retry is the same
        // create rather than a second document.
        Err(error) => publishing_retry(document_id, attempts, error.to_string()),
    }
}

/// Authorization and target verdicts do not improve by waiting.
fn permanent(error: &MetadataWriteError) -> bool {
    matches!(
        error,
        MetadataWriteError::Unauthorized
            | MetadataWriteError::Forbidden
            | MetadataWriteError::NotFound
    )
}

/// Backoff before the entry is minted: nothing has been forwarded yet.
fn retry_state(attempts: u32, reason: String) -> IntakeState {
    if attempts >= MAX_INTAKE_ATTEMPTS {
        return IntakeState::Failed {
            reason,
            retryable: true,
        };
    }
    IntakeState::Pending {
        due_at_ms: next_due(attempts),
        attempts,
        last_error: Some(reason),
    }
}

/// Backoff after a forward whose outcome is unknown. The minted id is kept so
/// the next attempt is the same create rather than a second document.
fn publishing_retry(document_id: Ulid, attempts: u32, reason: String) -> IntakeState {
    if attempts >= MAX_INTAKE_ATTEMPTS {
        return IntakeState::Failed {
            reason: format!("{reason} (document id {document_id})"),
            retryable: true,
        };
    }
    IntakeState::Publishing {
        document_id,
        due_at_ms: next_due(attempts),
        attempts,
    }
}

fn next_due(attempts: u32) -> u64 {
    unix_timestamp_millis().saturating_add(queue_retry_after_ms(attempts))
}

/// Re-arms the drain when the queue still holds entries.
pub async fn restore_intake_timer(storage: &StorageHandle, task_handle: &TaskHandle) {
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
            warn!(error = %error, "Failed to scan the device authoring intake");
            return;
        }
        other => {
            warn!(event = ?other, "Unexpected event while scanning the device authoring intake");
            return;
        }
    };
    if has_entries
        && let TaskEvent::Error { message, .. } = task_handle
            .schedule_timer_if_idle(TaskKey::DrainDeviceIntake, Duration::ZERO)
            .await
    {
        warn!(message = %message, "Failed to restore the device intake timer");
    }
}

#[cfg(test)]
mod tests {
    use super::{
        DrainOutcome, claim_state, drain_intake, permanent, publishing_retry, retry_state,
    };
    use crate::device::delete_draft::DeleteDraftOperation;
    use crate::device::inspect_draft::{InspectDraftError, InspectDraftOperation};
    use crate::device::repository::{IntakeEntry, IntakeState, MAX_INTAKE_ATTEMPTS, intake_entry};
    use crate::driver::{DriverContext, drive};
    use crate::metadata::forward::MetadataWriteError;
    use aruna_core::effects::StorageEffect;
    use aruna_core::structs::RealmId;
    use aruna_core::types::UserId;
    use aruna_storage::storage;
    use std::sync::Arc;
    use tempfile::tempdir;
    use ulid::Ulid;

    async fn context() -> (tempfile::TempDir, Arc<DriverContext>) {
        let tempdir = tempdir().unwrap();
        let storage_handle = storage::FjallStorage::open(tempdir.path().to_str().unwrap()).unwrap();
        (
            tempdir,
            Arc::new(DriverContext {
                storage_handle,
                net_handle: None,
                blob_handle: None,
                metadata_handle: None,
                task_handle: None,
                compute_handle: None,
            }),
        )
    }

    fn entry() -> IntakeEntry {
        IntakeEntry::new(
            Ulid::generate(),
            UserId::local(Ulid::generate(), RealmId::from_bytes([6u8; 32])),
            Ulid::generate(),
            "/notes".to_string(),
            false,
            "{}".to_string(),
        )
    }

    async fn store(context: &Arc<DriverContext>, entry: &IntakeEntry) {
        let (key_space, key, value) = intake_entry(entry).unwrap();
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

    fn publishing() -> IntakeState {
        IntakeState::Publishing {
            document_id: Ulid::from_bytes([4u8; 16]),
            due_at_ms: 1,
            attempts: 1,
        }
    }

    #[tokio::test]
    async fn defers_without_realm() {
        // No net handle means the realm is unreachable: nothing may be touched.
        let (_tempdir, context) = context().await;
        let entry = entry();
        store(&context, &entry).await;
        assert_eq!(drain_intake(&context).await, DrainOutcome::Deferred);
    }

    #[test]
    fn keeps_minted_id_on_retry() {
        // A forward with an unknown outcome must never mint a second id.
        let document_id = Ulid::generate();
        let IntakeState::Publishing {
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
            publishing_retry(document_id, MAX_INTAKE_ATTEMPTS, "unreachable".to_string()),
            IntakeState::Failed {
                retryable: true,
                ..
            }
        ));
        assert!(matches!(
            retry_state(MAX_INTAKE_ATTEMPTS, "no placement".to_string()),
            IntakeState::Failed {
                retryable: true,
                ..
            }
        ));
    }

    #[test]
    fn backs_off_before_minting() {
        let IntakeState::Pending {
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
        let entry = entry();
        let advanced = IntakeEntry {
            state: IntakeState::Failed {
                reason: "parked".to_string(),
                retryable: true,
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
