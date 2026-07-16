pub mod descriptor;
pub mod head_source;
pub mod read_source;
pub mod reference;
pub mod snapshot;

pub use descriptor::*;
pub use head_source::*;
pub use read_source::*;
pub use reference::*;
pub use snapshot::*;

use aruna_core::events::{Event, StagingSourceEvent, SubOperationEvent};

pub(crate) fn describe_event(event: &Event) -> String {
    match event {
        Event::Blob(_) => "Event::Blob".to_string(),
        Event::StagingSource(staging_event) => match staging_event {
            StagingSourceEvent::HeadResult { .. } => {
                "Event::StagingSource(StagingSourceEvent::HeadResult)".to_string()
            }
            StagingSourceEvent::ReadResult { .. } => {
                "Event::StagingSource(StagingSourceEvent::ReadResult)".to_string()
            }
            StagingSourceEvent::Error { .. } => {
                "Event::StagingSource(StagingSourceEvent::Error)".to_string()
            }
        },
        Event::Storage(_) => "Event::Storage".to_string(),
        Event::Net(_) => "Event::Net".to_string(),
        Event::Metadata(_) => "Event::Metadata".to_string(),
        Event::SubOperation(suboperation_event) => match suboperation_event {
            SubOperationEvent::DepthLimitExceeded { .. } => {
                "Event::SubOperation(SubOperationEvent::DepthLimitExceeded)".to_string()
            }
            SubOperationEvent::AuthorizationResult { .. } => {
                "Event::SubOperation(SubOperationEvent::AuthorizationResult)".to_string()
            }
            SubOperationEvent::RealmNodesResult { .. } => {
                "Event::SubOperation(SubOperationEvent::RealmNodesResult)".to_string()
            }
            SubOperationEvent::DocumentSyncResult { .. } => {
                "Event::SubOperation(SubOperationEvent::DocumentSyncResult)".to_string()
            }
            SubOperationEvent::SourceConnectorResolved { .. } => {
                "Event::SubOperation(SubOperationEvent::SourceConnectorResolved)".to_string()
            }
            SubOperationEvent::VersionSourceAccessResolved { .. } => {
                "Event::SubOperation(SubOperationEvent::VersionSourceAccessResolved)".to_string()
            }
            SubOperationEvent::ReplicationItemResult { .. } => {
                "Event::SubOperation(SubOperationEvent::ReplicationItemResult)".to_string()
            }
            SubOperationEvent::ReplicationTransferResult { .. } => {
                "Event::SubOperation(SubOperationEvent::ReplicationTransferResult)".to_string()
            }
            SubOperationEvent::ReplicationApplyResult { .. } => {
                "Event::SubOperation(SubOperationEvent::ReplicationApplyResult)".to_string()
            }
            SubOperationEvent::NotificationsEmitted => {
                "Event::SubOperation(SubOperationEvent::NotificationsEmitted)".to_string()
            }
        },
        Event::Task(_) => "Event::Task".to_string(),
        Event::Search() => "Event::Search".to_string(),
        Event::Stream() => "Event::Stream".to_string(),
    }
}

#[cfg(test)]
pub(crate) mod test_utils {
    use crate::connectors::create_source_connector::{
        CreateSourceConnectorInput, CreateSourceConnectorOperation,
    };
    use crate::driver::{DriverContext, drive};
    use aruna_blob::blob::BlobHandler;
    use aruna_core::structs::{Backend, BackendConfig, SourceConnector, SourceConnectorKind};
    use aruna_net::{NetConfig, NetHandle};
    use aruna_storage::storage;
    use std::collections::HashMap;
    use tempfile::TempDir;
    use ulid::Ulid;

    pub(crate) struct StagingTestContext {
        pub(crate) _tempdir: TempDir,
        pub(crate) driver_context: DriverContext,
    }

    pub(crate) async fn setup_driver_context() -> StagingTestContext {
        let tempdir = tempfile::tempdir().expect("tempdir must be created");
        let temp_root = tempdir.path().to_str().expect("temp path must be utf-8");
        let blob_root = format!("{temp_root}/blobstore");
        std::fs::create_dir_all(&blob_root).expect("blob root must be created");

        let storage_handle = storage::FjallStorage::open(temp_root).expect("storage must open");
        let net_handle = NetHandle::new(NetConfig::default(), storage_handle.clone())
            .await
            .expect("net handle must initialize");
        let blob_handle = BlobHandler::new(
            BackendConfig {
                backend_type: Backend::FileSystem,
                root: blob_root,
                service_config: HashMap::new(),
                bucket_prefix: Some("aruna-test-".to_string()),
                max_bucket_size: Some(1),
                multipart_bucket: Some("uploaded-parts".to_string()),
                timeouts: Default::default(),
            },
            storage_handle.clone(),
            net_handle,
        )
        .await
        .expect("blob handle must initialize");

        StagingTestContext {
            _tempdir: tempdir,
            driver_context: DriverContext {
                storage_handle,
                net_handle: None,
                blob_handle: Some(blob_handle),
                metadata_handle: None,
                task_handle: None,
                compute_handle: None,
            },
        }
    }

    pub(crate) async fn create_http_connector(
        context: &DriverContext,
        group_id: Ulid,
        endpoint: &str,
    ) -> SourceConnector {
        drive(
            CreateSourceConnectorOperation::new(CreateSourceConnectorInput {
                group_id,
                created_by: Default::default(),
                name: "http-source".to_string(),
                kind: SourceConnectorKind::Http,
                public_config: HashMap::from([("endpoint".to_string(), endpoint.to_string())]),
                secret_config: HashMap::new(),
            }),
            context,
        )
        .await
        .expect("connector creation must succeed")
        .connector
    }
}
