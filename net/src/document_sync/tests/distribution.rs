//! Tests that receiving nodes admit distribution changes only with group admin authority or
//! from the member's own node, and store the reduced record.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::keyspaces::DISTRIBUTION_KEYSPACE;
use aruna_core::structs::storage::distribution::presets::replica_layout;
use aruna_core::structs::storage::distribution::{
    DistributionChange, DistributionConfig, DistributionOp, DistributionRecord,
    DistributionRevision, DistributionSettings, SetPhase, owned_set_id,
};

struct Fixture {
    realm_id: RealmId,
    group_id: Ulid,
    set_id: Ulid,
    admin: Actor,
    member: Actor,
    other: Actor,
}

impl Fixture {
    fn event(
        &self,
        actor: &Actor,
        seq: u64,
        target: AdminDocumentTarget,
        op: AdminDocumentOperation,
    ) -> AdminDocumentEvent {
        let mut event = test_admin_event(Ulid::generate(), target, actor, seq, op);
        event.observed.advance(actor.node_id, seq - 1);
        event
    }

    fn change(
        &self,
        actor: &Actor,
        seq: u64,
        predecessor: Option<DistributionRevision>,
        op: DistributionOp,
    ) -> AdminDocumentEvent {
        let target = AdminDocumentTarget::Distribution {
            realm_id: self.realm_id,
            set_id: self.set_id,
        };
        let change = DistributionChange {
            set_id: self.set_id,
            predecessor,
            op,
        };
        let op = AdminDocumentOperation::DistributionChanged {
            change: Box::new(change),
        };
        self.event(actor, seq, target, op)
    }

    fn config(&self) -> DistributionConfig {
        let nodes = [
            (Ulid::from_parts(830, 1), self.admin.node_id),
            (Ulid::from_parts(830, 2), self.member.node_id),
        ];
        let layout = replica_layout(self.set_id, &nodes).unwrap();
        DistributionConfig {
            bucket: "spire".to_string(),
            owner_group: self.group_id,
            node_set: None,
            members: layout.members,
            ranges: layout.ranges,
            settings: DistributionSettings::default(),
        }
    }
}

async fn receive(storage: &StorageHandle, event: AdminDocumentEvent) -> AdminEventValidation {
    let realm_id = event.actor.realm_id;
    let target = match &event.target {
        AdminDocumentTarget::Group { group_id } => DocumentTarget::GroupAuthorization {
            group_id: *group_id,
        },
        _ => DocumentTarget::RealmConfig { realm_id },
    };
    let placement = admin_test_placement();
    let topic_id = target.sync_topic_id(realm_id, &placement);
    let outcome = validate_admin_event(
        storage,
        topic_id,
        ::irokle::actor_id_for(topic_id, node_to_peer(&event.origin_node_id)),
        &target,
        &event,
        realm_id,
        &placement,
        &sign_as_origin(&event, &placement),
        &mut ConfigValidationCache::default(),
    )
    .await
    .expect("validation runs");
    if outcome == AdminEventValidation::Accepted {
        apply_admin_operation(storage, target, event)
            .await
            .expect("accepted event applies");
    }
    outcome
}

async fn stored_record(storage: &StorageHandle, set_id: Ulid) -> Option<DistributionRecord> {
    storage_read_from(
        storage,
        DISTRIBUTION_KEYSPACE.to_string(),
        set_id.to_bytes().to_vec().into(),
    )
    .await
    .unwrap()
    .map(|value| DistributionRecord::from_bytes(&value).unwrap())
}

/// A realm with three server nodes and a group whose admin role the admin holds.
async fn fixture(storage: &StorageHandle) -> Fixture {
    let realm_id = RealmId::from_bytes([83; 32]);
    let user = |seed| UserId::local(Ulid::from_parts(831, seed), realm_id);
    let fixture = Fixture {
        realm_id,
        group_id: Ulid::from_parts(832, 1),
        set_id: owned_set_id(Ulid::from_parts(832, 1), 833, 1),
        admin: test_actor(31, user(1), realm_id),
        member: test_actor(32, user(2), realm_id),
        other: test_actor(33, user(3), realm_id),
    };
    let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
    for actor in [&fixture.admin, &fixture.member, &fixture.other] {
        config.ensure_node(actor.node_id, RealmNodeKind::Server);
    }
    let realm_auth = RealmAuthorizationDocument {
        realm_id,
        roles: HashMap::new(),
        operation_restrictions: HashMap::new(),
    };
    batch_write_to(
        storage,
        vec![
            target_write_entry(
                DocumentTarget::RealmConfig { realm_id },
                config.to_bytes(&fixture.admin).unwrap().into(),
            ),
            target_write_entry(
                DocumentTarget::RealmAuthorization { realm_id },
                realm_auth.to_bytes(&fixture.admin).unwrap().into(),
            ),
        ],
    )
    .await
    .unwrap();
    let group = AdminDocumentTarget::Group {
        group_id: fixture.group_id,
    };
    let role_id = Ulid::from_parts(834, 1);
    let subtree = aruna_core::permission_path::role_subtree_root(realm_id, fixture.group_id);
    let ops = [
        AdminDocumentOperation::GroupCreated {
            realm_id,
            display_name: "Spire".into(),
            owner: fixture.admin.user_id,
        },
        AdminDocumentOperation::GroupRoleCreated {
            role: admin_role(
                role_id,
                "admin",
                &format!("{subtree}/**"),
                Permission::WRITE,
            ),
        },
        AdminDocumentOperation::GroupAssignmentAdded {
            role_id,
            user_id: fixture.admin.user_id,
        },
    ];
    for (seq, op) in (1..).zip(ops) {
        let event = fixture.event(&fixture.admin, seq, group.clone(), op);
        assert_eq!(
            receive(storage, event).await,
            AdminEventValidation::Accepted
        );
    }
    fixture
}

fn rejected(outcome: AdminEventValidation) -> bool {
    matches!(outcome, AdminEventValidation::Rejected(_))
}

#[tokio::test]
async fn admits_authorized_changes() {
    let (_dir, storage) = test_storage();
    let fx = fixture(&storage).await;
    let create = DistributionOp::Create {
        config: fx.config(),
    };
    let foreign_create = fx.change(&fx.other, 1, None, create.clone());
    assert!(rejected(receive(&storage, foreign_create).await));
    let created = fx.change(&fx.admin, 4, None, create);
    assert_eq!(
        receive(&storage, created).await,
        AdminEventValidation::Accepted
    );
    let record = stored_record(&storage, fx.set_id)
        .await
        .expect("record stored");
    assert_eq!(
        (record.phase, record.revision.number),
        (SetPhase::Pending, 1)
    );

    let settings = DistributionOp::ChangeSettings {
        settings: DistributionSettings {
            public: true,
            ..DistributionSettings::default()
        },
    };
    let foreign_settings = fx.change(&fx.other, 2, Some(record.revision), settings);
    assert!(rejected(receive(&storage, foreign_settings).await));

    let member_id = Ulid::from_parts(830, 2);
    let admit = DistributionOp::Admit {
        member_id,
        incarnation: 5,
    };
    let relayed = fx.change(&fx.other, 3, Some(record.revision), admit.clone());
    assert!(rejected(receive(&storage, relayed).await));
    let own = fx.change(&fx.member, 1, Some(record.revision), admit);
    assert_eq!(receive(&storage, own).await, AdminEventValidation::Accepted);
    let admitted = stored_record(&storage, fx.set_id).await.unwrap();
    assert_eq!(admitted.member(member_id).unwrap().incarnation, Some(5));

    let leave = DistributionOp::Leave { member_id };
    let unsupported = fx.change(&fx.admin, 5, Some(admitted.revision), leave);
    assert!(rejected(receive(&storage, unsupported).await));
    // A later event on a revision that already has a successor can never win.
    let mut stale = fx.change(
        &fx.admin,
        5,
        Some(record.revision),
        DistributionOp::Activate,
    );
    stale.event_id = Ulid::from_parts((1 << 48) - 1, 0);
    assert!(rejected(receive(&storage, stale).await));
}
