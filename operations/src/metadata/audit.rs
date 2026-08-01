//! Read surface over the metadata audit trail. Records are written by the
//! mutation paths; this module only pages through them.

use crate::driver::DriverContext;
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::METADATA_AUDIT_KEYSPACE;
use aruna_core::structs::MetadataAuditRecord;
use aruna_core::types::GroupId;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ulid::Ulid;

pub const MAX_AUDIT_PAGE_SIZE: usize = 200;
pub const DEFAULT_AUDIT_PAGE_SIZE: usize = 50;

#[derive(Debug, Clone)]
pub struct ListAuditRequest {
    pub group_id: GroupId,
    pub document_id: Option<Ulid>,
    pub cursor: Option<String>,
    pub limit: Option<usize>,
}

#[derive(Debug)]
pub struct AuditPage {
    pub records: Vec<MetadataAuditRecord>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum ListAuditError {
    #[error("invalid audit cursor")]
    InvalidCursor,
    #[error("audit read failed: {0}")]
    Storage(String),
}

/// Pages the audit records of one group, optionally narrowed to one document,
/// in key order (document id, then audit id).
pub async fn list_audit_records(
    context: &DriverContext,
    request: ListAuditRequest,
) -> Result<AuditPage, ListAuditError> {
    let mut prefix = request.group_id.to_bytes().to_vec();
    if let Some(document_id) = request.document_id {
        prefix.extend_from_slice(&document_id.to_bytes());
    }
    let start = request
        .cursor
        .as_deref()
        .map(|cursor| {
            URL_SAFE_NO_PAD
                .decode(cursor)
                .map_err(|_| ListAuditError::InvalidCursor)
        })
        .transpose()?
        .map(|key| IterStart::After(key.into()));
    let limit = request
        .limit
        .unwrap_or(DEFAULT_AUDIT_PAGE_SIZE)
        .clamp(1, MAX_AUDIT_PAGE_SIZE);

    let event = context
        .storage_handle
        .send_effect(Effect::Storage(StorageEffect::Iter {
            key_space: METADATA_AUDIT_KEYSPACE.to_string(),
            prefix: Some(prefix.into()),
            start,
            limit,
            txn_id: None,
        }))
        .await;
    let (values, next_start_after) = match event {
        Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) => (values, next_start_after),
        Event::Storage(StorageEvent::Error { error }) => {
            return Err(ListAuditError::Storage(error.to_string()));
        }
        other => {
            return Err(ListAuditError::Storage(format!(
                "unexpected event {other:?}"
            )));
        }
    };

    let records = values
        .iter()
        .map(|(_, value)| {
            postcard::from_bytes::<MetadataAuditRecord>(value)
                .map_err(|error| ListAuditError::Storage(error.to_string()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(AuditPage {
        records,
        next_cursor: next_start_after.map(|key| URL_SAFE_NO_PAD.encode(&key)),
    })
}

#[cfg(test)]
mod tests {
    use super::{ListAuditRequest, list_audit_records};
    use crate::driver::DriverContext;
    use crate::metadata::repository::write_audit_effect;
    use aruna_core::UserId;
    use aruna_core::handle::Handle;
    use aruna_core::structs::{MetadataAuditOperation, MetadataAuditRecord, RealmId};
    use aruna_storage::storage::FjallStorage;
    use aruna_tasks::TaskHandle;
    use tempfile::tempdir;
    use ulid::Ulid;

    fn record(group_id: Ulid, document_id: Ulid, realm_id: RealmId) -> MetadataAuditRecord {
        MetadataAuditRecord {
            realm_id,
            group_id,
            document_id,
            graph_iri: "urn:test".to_string(),
            user_id: UserId::local(Ulid::from_bytes([1u8; 16]), realm_id),
            node_id: iroh::SecretKey::from_bytes(&[2u8; 32]).public(),
            operation: MetadataAuditOperation::Create,
            occurred_at_ms: 1,
            details: None,
        }
    }

    #[tokio::test]
    async fn pages_audit_records() {
        // Cursor paging walks the group's records; a document filter narrows.
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle: storage,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: Some(TaskHandle::new()),
            compute_handle: None,
        };
        let realm_id = RealmId([9u8; 32]);
        let group_id = Ulid::from_bytes([3u8; 16]);
        let first_doc = Ulid::from_bytes([4u8; 16]);
        let second_doc = Ulid::from_bytes([5u8; 16]);
        for (document_id, audit_id) in [
            (first_doc, Ulid::from_bytes([6u8; 16])),
            (first_doc, Ulid::from_bytes([7u8; 16])),
            (second_doc, Ulid::from_bytes([8u8; 16])),
        ] {
            let effect =
                write_audit_effect(&record(group_id, document_id, realm_id), audit_id, None)
                    .unwrap();
            context.storage_handle.send_effect(effect).await;
        }

        let first_page = list_audit_records(
            &context,
            ListAuditRequest {
                group_id,
                document_id: None,
                cursor: None,
                limit: Some(2),
            },
        )
        .await
        .unwrap();
        assert_eq!(first_page.records.len(), 2);
        let cursor = first_page.next_cursor.expect("more records");

        let second_page = list_audit_records(
            &context,
            ListAuditRequest {
                group_id,
                document_id: None,
                cursor: Some(cursor),
                limit: Some(2),
            },
        )
        .await
        .unwrap();
        assert_eq!(second_page.records.len(), 1);
        assert_eq!(second_page.records[0].document_id, second_doc);

        let filtered = list_audit_records(
            &context,
            ListAuditRequest {
                group_id,
                document_id: Some(second_doc),
                cursor: None,
                limit: None,
            },
        )
        .await
        .unwrap();
        assert_eq!(filtered.records.len(), 1);

        let foreign = list_audit_records(
            &context,
            ListAuditRequest {
                group_id: Ulid::from_bytes([99u8; 16]),
                document_id: None,
                cursor: None,
                limit: None,
            },
        )
        .await
        .unwrap();
        assert!(foreign.records.is_empty());
    }
}
