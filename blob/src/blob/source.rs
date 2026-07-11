use super::BlobHandler;
use crate::opendal::{head_staging_source, read_staging_source};
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StagingSourceEvent, StorageEvent};
use aruna_core::keyspaces::REALM_CONFIG_KEYSPACE;
use aruna_core::structs::{RealmConfigDocument, ResolvedSourceAccess};
use aruna_egress::EgressPolicy;

impl BlobHandler {
    /// Loads the realm egress allowlist fresh per fetch (a node holds one realm
    /// config); a missing document falls back to the built-in deny table.
    async fn load_egress_policy(&self) -> EgressPolicy {
        match self
            .storage
            .send_storage_effect(StorageEffect::Iter {
                key_space: REALM_CONFIG_KEYSPACE.to_string(),
                prefix: None,
                start: None,
                limit: 1,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::IterResult { values, .. }) => values
                .first()
                .and_then(|(_, value)| RealmConfigDocument::from_bytes(value).ok())
                .map(|document| EgressPolicy::from_config(&document.egress))
                .unwrap_or_default(),
            _ => EgressPolicy::deny_all(),
        }
    }

    pub(crate) async fn head_staging_source(
        &self,
        access: ResolvedSourceAccess,
    ) -> StagingSourceEvent {
        let policy = self.load_egress_policy().await;
        match head_staging_source(&access, &policy, None).await {
            Ok(metadata) => StagingSourceEvent::HeadResult { metadata },
            Err(error) => StagingSourceEvent::Error { error },
        }
    }

    pub(crate) async fn read_staging_source(
        &self,
        access: ResolvedSourceAccess,
        range: Option<std::ops::Range<u64>>,
    ) -> StagingSourceEvent {
        let policy = self.load_egress_policy().await;
        match read_staging_source(&access, range, &policy, None).await {
            Ok((metadata, stream)) => StagingSourceEvent::ReadResult { metadata, stream },
            Err(error) => StagingSourceEvent::Error { error },
        }
    }
}
