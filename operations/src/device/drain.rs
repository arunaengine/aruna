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
    IntakeEntry, IntakeState, MAX_INTAKE_ATTEMPTS, entry_with_state, intake_entry, scan_intake,
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

/// Forwards every due entry of one page, oldest first.
pub async fn drain_intake(context: &Arc<DriverContext>) -> DrainOutcome {
    let Some(net_handle) = context.net_handle.as_ref() else {
        return DrainOutcome::Deferred;
    };
    let realm_id = *net_handle.realm_id();
    let node_id = net_handle.node_id();
    let Some(config) = load_realm_config(context, realm_id).await else {
        return DrainOutcome::Deferred;
    };

    let entries = match read_page(context).await {
        Some(entries) => entries,
        None => return DrainOutcome::Deferred,
    };
    let now = unix_timestamp_millis();
    let mut due = false;
    for entry in entries {
        if !entry.is_due(now) {
            continue;
        }
        due = true;
        let next = publish_entry(context, &config, realm_id, node_id, &entry).await;
        store_entry(context, &entry_with_state(&entry, next)).await;
    }
    if due {
        DrainOutcome::More
    } else {
        DrainOutcome::Idle
    }
}

/// The oldest page of the queue. `None` means the scan itself failed.
async fn read_page(context: &Arc<DriverContext>) -> Option<Vec<IntakeEntry>> {
    let Effect::Storage(effect) = scan_intake(None, None) else {
        return None;
    };
    match context.storage_handle.send_storage_effect(effect).await {
        Event::Storage(StorageEvent::IterResult { values, .. }) => Some(
            values
                .into_iter()
                .filter_map(|(_, bytes)| IntakeEntry::from_bytes(&bytes).ok())
                .collect(),
        ),
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

/// Forwards one entry and answers with the state it must be stored under.
async fn publish_entry(
    context: &Arc<DriverContext>,
    config: &RealmConfigDocument,
    realm_id: RealmId,
    node_id: aruna_core::NodeId,
    entry: &IntakeEntry,
) -> IntakeState {
    let actor = Actor {
        node_id,
        user_id: entry.owner,
        realm_id,
    };
    let attempts = entry.attempts().saturating_add(1);
    let document_id = match &entry.state {
        IntakeState::Publishing { document_id, .. } => *document_id,
        _ => match mint_forward_document(config, &actor, entry.group_id, &entry.document_path) {
            Ok(minted) => minted.as_ulid(),
            // Placement or configuration is not settled yet; try again later.
            Err(error) => return retry_state(attempts, error.to_string()),
        },
    };

    // The minted id is stored before the forward, so a crash mid-publish
    // re-forwards the same id and the holder answers from its create fence.
    store_entry(
        context,
        &entry_with_state(
            entry,
            IntakeState::Publishing {
                document_id,
                due_at_ms: unix_timestamp_millis(),
                attempts,
            },
        ),
    )
    .await;

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
    use super::{DrainOutcome, drain_intake, permanent, publishing_retry, retry_state};
    use crate::device::repository::{IntakeEntry, IntakeState, MAX_INTAKE_ATTEMPTS, intake_entry};
    use crate::driver::DriverContext;
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

    #[tokio::test]
    async fn defers_without_realm() {
        // No net handle means the realm is unreachable: nothing may be touched.
        let (_tempdir, context) = context().await;
        let entry = entry();
        let (key_space, key, value) = intake_entry(&entry).unwrap();
        context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space,
                key,
                value,
                txn_id: None,
            })
            .await;
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

    #[test]
    fn classifies_permanent_errors() {
        assert!(permanent(&MetadataWriteError::Forbidden));
        assert!(!permanent(&MetadataWriteError::Undeliverable(
            "no holder".to_string()
        )));
    }
}
