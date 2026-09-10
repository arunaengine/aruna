use aruna_core::document::DocumentSyncTarget;
use tokio::sync::watch;

use crate::driver::DriverContext;

pub fn subscribe_dashboard_changes(
    context: &DriverContext,
) -> Option<(String, watch::Receiver<u64>)> {
    context.net_handle.as_ref().map(|net_handle| {
        (
            net_handle.dashboard_epoch().to_string(),
            net_handle.subscribe_dashboard_changes(),
        )
    })
}

pub fn notify_dashboard_change(context: &DriverContext) {
    if let Some(net_handle) = context.net_handle.as_ref() {
        net_handle.notify_dashboard_change();
    }
}

pub(crate) fn targets_change_dashboard(targets: &[DocumentSyncTarget]) -> bool {
    targets.iter().any(|target| {
        matches!(
            target,
            DocumentSyncTarget::Group { .. }
                | DocumentSyncTarget::GroupAuthorization { .. }
                | DocumentSyncTarget::RealmAuthorization { .. }
                | DocumentSyncTarget::RealmConfig { .. }
                | DocumentSyncTarget::User { .. }
                | DocumentSyncTarget::MetadataRegistry { .. }
                | DocumentSyncTarget::MetadataCreateEvent { .. }
                | DocumentSyncTarget::MetadataDocumentLifecycle { .. }
                | DocumentSyncTarget::MetadataGraphLifecycle { .. }
                | DocumentSyncTarget::NodeUsage { .. }
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::UserId;
    use aruna_core::structs::RealmId;
    use ulid::Ulid;

    #[test]
    fn target_filtering() {
        let realm_id = RealmId::from_bytes([1; 32]);
        let node_id = iroh::SecretKey::from_bytes(&[2; 32]).public();
        let group_id = Ulid::from_parts(3, 1);
        let user_id = UserId::new(Ulid::from_parts(4, 1), realm_id);
        let document_id = Ulid::from_parts(5, 1);
        let relevant = [
            DocumentSyncTarget::Group { group_id },
            DocumentSyncTarget::GroupAuthorization { group_id },
            DocumentSyncTarget::RealmAuthorization { realm_id },
            DocumentSyncTarget::RealmConfig { realm_id },
            DocumentSyncTarget::User { user_id },
            DocumentSyncTarget::MetadataRegistry {
                group_id,
                document_id,
            },
            DocumentSyncTarget::MetadataCreateEvent {
                document_id,
                event_id: Ulid::from_parts(6, 1),
            },
            DocumentSyncTarget::MetadataDocumentLifecycle { document_id },
            DocumentSyncTarget::MetadataGraphLifecycle {
                graph_iri: "https://example.test/graph".to_string(),
            },
            DocumentSyncTarget::NodeUsage {
                realm_id,
                node_id,
                group_id: None,
            },
        ];
        assert!(
            relevant
                .iter()
                .all(|target| targets_change_dashboard(std::slice::from_ref(target)))
        );

        let ignored = [
            DocumentSyncTarget::WatchInterest { realm_id, node_id },
            DocumentSyncTarget::WatchSubscription {
                owner: user_id,
                watch_id: Ulid::from_parts(7, 1),
            },
            DocumentSyncTarget::NodeInfo { realm_id, node_id },
        ];
        assert!(!targets_change_dashboard(&ignored));
    }
}
