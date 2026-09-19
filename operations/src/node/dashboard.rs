//! Signals and subscribes to dashboard changes and tells which targets affect them.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::document::DocumentTarget;
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

pub(crate) fn targets_change_dashboard(targets: &[DocumentTarget]) -> bool {
    targets.iter().any(|target| {
        matches!(
            target,
            DocumentTarget::Group { .. }
                | DocumentTarget::GroupAuthorization { .. }
                | DocumentTarget::RealmAuthorization { .. }
                | DocumentTarget::RealmConfig { .. }
                | DocumentTarget::User { .. }
                | DocumentTarget::MetadataRegistry { .. }
                | DocumentTarget::MetadataCreateEvent { .. }
                | DocumentTarget::MetadataDocumentLifecycle { .. }
                | DocumentTarget::MetadataGraphLifecycle { .. }
                | DocumentTarget::NodeUsage { .. }
        )
    })
}

#[cfg(test)]
mod pure_tests {
    use super::*;
    use aruna_core::UserId;
    use aruna_core::structs::identity::realm::RealmId;
    use ulid::Ulid;

    #[test]
    fn target_filtering() {
        let realm_id = RealmId::from_bytes([1; 32]);
        let node_id = iroh::SecretKey::from_bytes(&[2; 32]).public();
        let group_id = Ulid::from_parts(3, 1);
        let user_id = UserId::new(Ulid::from_parts(4, 1), realm_id);
        let document_id = Ulid::from_parts(5, 1);
        let relevant = [
            DocumentTarget::Group { group_id },
            DocumentTarget::GroupAuthorization { group_id },
            DocumentTarget::RealmAuthorization { realm_id },
            DocumentTarget::RealmConfig { realm_id },
            DocumentTarget::User { user_id },
            DocumentTarget::MetadataRegistry {
                group_id,
                document_id,
            },
            DocumentTarget::MetadataCreateEvent {
                document_id,
                event_id: Ulid::from_parts(6, 1),
            },
            DocumentTarget::MetadataDocumentLifecycle { document_id },
            DocumentTarget::MetadataGraphLifecycle {
                graph_iri: "https://example.test/graph".to_string(),
            },
            DocumentTarget::NodeUsage {
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
            DocumentTarget::WatchInterest { realm_id, node_id },
            DocumentTarget::WatchSubscription {
                owner: user_id,
                watch_id: Ulid::from_parts(7, 1),
            },
            DocumentTarget::NodeInfo { realm_id, node_id },
        ];
        assert!(!targets_change_dashboard(&ignored));
    }
}
