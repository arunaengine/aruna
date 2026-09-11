pub mod check_source;
pub mod descriptor;
pub mod head_source;
pub mod list_source;
pub mod native_source;
pub mod offered_directory;
pub mod read_source;
pub mod reference;
pub mod snapshot;

pub use check_source::*;
pub use descriptor::*;
pub use head_source::*;
pub use list_source::*;
pub use offered_directory::*;
pub use read_source::*;
pub use reference::*;
pub use snapshot::*;

use aruna_core::events::{Event, StagingSourceEvent, SubOperationEvent};

pub(crate) fn describe_event(event: &Event) -> String {
    match event {
        Event::Blob(_) => "Event::Blob".to_string(),
        Event::LocalFile(_) => "Event::LocalFile".to_string(),
        Event::StagingSource(staging_event) => match staging_event {
            StagingSourceEvent::CheckResult => {
                "Event::StagingSource(StagingSourceEvent::CheckResult)".to_string()
            }
            StagingSourceEvent::HeadResult { .. } => {
                "Event::StagingSource(StagingSourceEvent::HeadResult)".to_string()
            }
            StagingSourceEvent::ListResult { .. } => {
                "Event::StagingSource(StagingSourceEvent::ListResult)".to_string()
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
            SubOperationEvent::LiveReplicationQueued { .. } => {
                "Event::SubOperation(SubOperationEvent::LiveReplicationQueued)".to_string()
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
            SubOperationEvent::BucketCreated { .. } => {
                "Event::SubOperation(SubOperationEvent::BucketCreated)".to_string()
            }
            SubOperationEvent::GroupRoutingLoaded { .. } => {
                "Event::SubOperation(SubOperationEvent::GroupRoutingLoaded)".to_string()
            }
            SubOperationEvent::NotificationsEmitted => {
                "Event::SubOperation(SubOperationEvent::NotificationsEmitted)".to_string()
            }
            SubOperationEvent::TokenRevoked { .. } => {
                "Event::SubOperation(SubOperationEvent::TokenRevoked)".to_string()
            }
        },
        Event::Task(_) => "Event::Task".to_string(),
        Event::Search() => "Event::Search".to_string(),
        Event::Stream() => "Event::Stream".to_string(),
    }
}

#[cfg(test)]
pub(crate) mod tests;
