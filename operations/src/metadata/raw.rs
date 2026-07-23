use aruna_core::effects::{IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::METADATA_EVENT_LOG_KEYSPACE;
use aruna_core::metadata::{
    MetadataCreateEventRecord, MetadataError, MetadataMaterializationState, MetadataRawRevision,
    resolve_raw_revision,
};
use aruna_core::storage_entries::metadata_event_log_prefix;
use aruna_core::types::Key;
use thiserror::Error;
use ulid::Ulid;

use crate::driver::DriverContext;
use crate::metadata::repository::{
    parse_materialization_status_read, read_materialization_status_effect,
};

const RAW_PAGE_SIZE: usize = 1_024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataRawView {
    pub revision: MetadataRawRevision,
    pub projection_state: MetadataMaterializationState,
    pub projected_event_id: Option<Ulid>,
}

#[derive(Debug, Error)]
pub enum MetadataRawReadError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Metadata(#[from] MetadataError),
    #[error("unexpected metadata raw-read event: {0}")]
    UnexpectedEvent(String),
}

pub async fn load_raw_view(
    context: &DriverContext,
    document_id: Ulid,
) -> Result<Option<MetadataRawView>, MetadataRawReadError> {
    let Some(revision) = load_raw_revision(context, document_id, None).await? else {
        return Ok(None);
    };
    let status = parse_materialization_status_read(
        context
            .storage_handle
            .send_effect(read_materialization_status_effect(document_id, None))
            .await,
    )
    .map_err(|error| match error {
        crate::metadata::repository::StorageReadError::Storage(error) => {
            MetadataRawReadError::Storage(error)
        }
        crate::metadata::repository::StorageReadError::Conversion(error) => {
            MetadataRawReadError::Conversion(error)
        }
    })?;
    let projection_state = status
        .as_ref()
        .map(|status| status.state)
        .unwrap_or(MetadataMaterializationState::Pending);
    let projected_event_id = status
        .filter(|status| {
            status.state == MetadataMaterializationState::Materialized
                && status.event_id == revision.winning_event_id
                && status.context_digest == Some(revision.context_digest)
                && status.dataset_digest == revision.dataset_digest
        })
        .map(|status| status.event_id);
    Ok(Some(MetadataRawView {
        revision,
        projection_state,
        projected_event_id,
    }))
}

pub async fn load_raw_revision(
    context: &DriverContext,
    document_id: Ulid,
    event_cursor: Option<Ulid>,
) -> Result<Option<MetadataRawRevision>, MetadataRawReadError> {
    let prefix = metadata_event_log_prefix(document_id);
    let mut start: Option<Key> = None;
    let mut events = Vec::new();
    loop {
        let event = context
            .storage_handle
            .send_storage_effect(StorageEffect::Iter {
                key_space: METADATA_EVENT_LOG_KEYSPACE.to_string(),
                prefix: Some(prefix.clone()),
                start: start.take().map(IterStart::After),
                limit: RAW_PAGE_SIZE,
                txn_id: None,
            })
            .await;
        let (values, next_start_after) = match event {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) => (values, next_start_after),
            Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
            other => {
                return Err(MetadataRawReadError::UnexpectedEvent(format!("{other:?}")));
            }
        };
        for (_, value) in values {
            let event: MetadataCreateEventRecord =
                postcard::from_bytes(&value).map_err(ConversionError::from)?;
            if event.record.document_id != document_id {
                return Err(MetadataRawReadError::UnexpectedEvent(format!(
                    "raw event belongs to document {}",
                    event.record.document_id
                )));
            }
            if event_cursor.is_none_or(|cursor| event.event_id <= cursor) {
                events.push(event);
            }
        }
        let Some(next) = next_start_after else {
            break;
        };
        start = Some(next);
    }
    resolve_raw_revision(&events).map_err(Into::into)
}
