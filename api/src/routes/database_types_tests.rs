//! Tests the database route shapes: target conversion, listing states and request views.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::UserId;
use aruna_core::structs::identity::realm::RealmId;

fn node() -> aruna_core::NodeId {
    iroh::SecretKey::from_bytes(&[31; 32]).public()
}

fn entry(target: DatabaseTarget) -> DatabaseEntry {
    DatabaseEntry {
        entry_id: Ulid::from_bytes([1; 16]),
        dataset_id: Ulid::from_bytes([2; 16]),
        target,
        owner_group: Ulid::from_bytes([3; 16]),
    }
}

#[test]
fn targets_round_trip() {
    for target in [
        DatabaseTarget::Distributed {
            set_id: Ulid::from_bytes([4; 16]),
        },
        DatabaseTarget::Ordinary {
            node_id: node(),
            bucket: "spire".to_string(),
        },
    ] {
        let body = DatabaseTargetBody::from(&target);
        assert_eq!(DatabaseTarget::try_from(body).unwrap(), target);
    }
}

#[test]
fn bad_targets_refused() {
    let set = DatabaseTargetBody::Distributed {
        set_id: "not-a-ulid".to_string(),
    };
    assert!(matches!(
        DatabaseTarget::try_from(set),
        Err(ServerError::BadRequestReason(reason)) if reason == "invalid set_id"
    ));
    let ordinary = DatabaseTargetBody::Ordinary {
        node_id: "nope".to_string(),
        bucket: "spire".to_string(),
    };
    assert!(matches!(
        DatabaseTarget::try_from(ordinary),
        Err(ServerError::BadRequestReason(reason)) if reason == "invalid node_id"
    ));
}

#[test]
fn listing_states_named() {
    let states = [
        (DatabaseState::Active, "active"),
        (DatabaseState::Unavailable, "description_unavailable"),
        (DatabaseState::DatasetHidden, "dataset_hidden"),
        (DatabaseState::Unlinked, "no_longer_linked"),
    ];
    for (state, name) in states {
        let listing = DatabaseListing {
            entry: entry(DatabaseTarget::Distributed {
                set_id: Ulid::from_bytes([4; 16]),
            }),
            bucket: "spire".to_string(),
            state,
            title: Some("SPIRE".to_string()),
            graph_iri: Some("https://w3id.org/aruna/x".to_string()),
            document_path: Some("databases/spire".to_string()),
            public: true,
        };
        let view = DatabaseView::from(listing);
        assert_eq!(view.state, name);
        assert_eq!(view.bucket, "spire");
        assert_eq!(view.title.as_deref(), Some("SPIRE"));
        assert!(view.public);
        assert_eq!(
            view.entry.owner_group,
            Ulid::from_bytes([3; 16]).to_string()
        );
    }
}

#[test]
fn request_view_fields() {
    let realm_id = RealmId::from_bytes([5; 32]);
    let curator = UserId::local(Ulid::from_bytes([6; 16]), realm_id);
    let request = DatabaseRequest {
        request_id: Ulid::from_bytes([7; 16]),
        entry: entry(DatabaseTarget::Ordinary {
            node_id: node(),
            bucket: "lab".to_string(),
        }),
        predecessor: None,
        curator,
        created_at_ms: 1_761_300_000_000,
    };
    let view = DatabaseRequestView::from(&request);
    assert_eq!(view.request_id, Ulid::from_bytes([7; 16]).to_string());
    assert_eq!(view.curator, curator.to_string());
    assert_eq!(view.created_at_ms, 1_761_300_000_000);
    assert!(matches!(
        view.entry.target,
        DatabaseTargetBody::Ordinary { ref bucket, .. } if bucket == "lab"
    ));
}
