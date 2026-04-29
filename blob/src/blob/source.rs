use super::BlobHandler;
use crate::opendal::{head_staging_source, read_staging_source};
use aruna_core::events::StagingSourceEvent;
use aruna_core::structs::ResolvedSourceAccess;

impl BlobHandler {
    pub(crate) async fn head_staging_source(
        &self,
        access: ResolvedSourceAccess,
    ) -> StagingSourceEvent {
        match head_staging_source(&access).await {
            Ok(metadata) => StagingSourceEvent::HeadResult { metadata },
            Err(error) => StagingSourceEvent::Error { error },
        }
    }

    pub(crate) async fn read_staging_source(
        &self,
        access: ResolvedSourceAccess,
        range: Option<std::ops::Range<u64>>,
    ) -> StagingSourceEvent {
        match read_staging_source(&access, range).await {
            Ok((metadata, stream)) => StagingSourceEvent::ReadResult { metadata, stream },
            Err(error) => StagingSourceEvent::Error { error },
        }
    }
}
