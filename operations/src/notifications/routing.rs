use aruna_core::structs::{
    GroupAuthorizationDocument, NotificationClass, NotificationKind, NotificationRecord,
    RealmAuthorizationDocument, ResourceEvent, WatchEvent, WatchSubscription,
    watch_notification_id,
};
use aruna_core::types::UserId;

pub fn group_admin_user_ids(auth_doc: &GroupAuthorizationDocument) -> Vec<UserId> {
    let mut ids: Vec<UserId> = auth_doc
        .roles
        .values()
        .filter(|role| role.name == "admin")
        .flat_map(|role| role.assigned_users.iter().copied())
        .collect();
    ids.sort();
    ids.dedup();
    ids
}

pub fn realm_admin_user_ids(auth_doc: &RealmAuthorizationDocument) -> Vec<UserId> {
    let mut ids: Vec<UserId> = auth_doc
        .roles
        .values()
        .filter(|role| role.name == "realm_admin")
        .flat_map(|role| role.assigned_users.iter().copied())
        .collect();
    ids.sort();
    ids.dedup();
    ids
}

pub struct RoutingContext<'a> {
    pub group_auth: Option<&'a GroupAuthorizationDocument>,
    pub realm_auth: Option<&'a RealmAuthorizationDocument>,
}

pub fn route_resource_event(
    event: &ResourceEvent,
    ctx: RoutingContext<'_>,
    now_ms: u64,
) -> Vec<NotificationRecord> {
    let mut records = Vec::new();
    match event {
        ResourceEvent::GroupMemberAdded {
            group_id,
            affected_user,
            actor_user_id,
        } => {
            if affected_user != actor_user_id {
                records.push(NotificationRecord::new(
                    *affected_user,
                    NotificationClass::Direct,
                    NotificationKind::AddedToGroup {
                        group_id: *group_id,
                        actor_user_id: *actor_user_id,
                    },
                    now_ms,
                ));
            }
            if let Some(group_auth) = ctx.group_auth {
                for admin in group_admin_user_ids(group_auth) {
                    if admin == *affected_user || admin == *actor_user_id {
                        continue;
                    }
                    records.push(NotificationRecord::new(
                        admin,
                        NotificationClass::Direct,
                        NotificationKind::GroupMemberAdded {
                            group_id: *group_id,
                            member_user_id: *affected_user,
                            actor_user_id: *actor_user_id,
                        },
                        now_ms,
                    ));
                }
            }
        }
        ResourceEvent::GroupMemberRemoved {
            group_id,
            affected_user,
            actor_user_id,
        } => {
            if affected_user != actor_user_id {
                records.push(NotificationRecord::new(
                    *affected_user,
                    NotificationClass::Direct,
                    NotificationKind::RemovedFromGroup {
                        group_id: *group_id,
                        actor_user_id: *actor_user_id,
                    },
                    now_ms,
                ));
            }
        }
        ResourceEvent::NodeOnboarded { realm_id, node_id } => {
            if let Some(realm_auth) = ctx.realm_auth {
                for admin in realm_admin_user_ids(realm_auth) {
                    records.push(NotificationRecord::new(
                        admin,
                        NotificationClass::Direct,
                        NotificationKind::NodeOnboarded {
                            realm_id: *realm_id,
                            node_id: *node_id,
                        },
                        now_ms,
                    ));
                }
            }
        }
    }
    records
}

/// Expands one origin watch event into recipient-addressed records against the
/// holder's local watch subscriptions. A subscription matches when the event
/// path starts with its prefix, its mask selects the event kind, it existed when
/// the event occurred, and its owner is not the actor (no self-notify). Each
/// record's id is deterministic in `(event_id, watch_id)` and its timestamp is
/// the event's, so re-expanding a redelivered event mints identical records for
/// the holder's idempotent upsert.
pub fn route_watch_event(
    event: &WatchEvent,
    subscriptions: &[WatchSubscription],
) -> Vec<NotificationRecord> {
    let mut records = Vec::new();
    for subscription in subscriptions {
        if subscription.created_at_ms > event.occurred_at_ms {
            continue;
        }
        if subscription.owner == event.actor {
            continue;
        }
        if !event.path.starts_with(&subscription.path_prefix) {
            continue;
        }
        if !subscription.event_mask.contains(event.kind) {
            continue;
        }
        records.push(NotificationRecord {
            notification_id: watch_notification_id(event.event_id, subscription.watch_id),
            recipient: subscription.owner,
            class: NotificationClass::Transient,
            kind: event.notification_kind(),
            created_at_ms: event.occurred_at_ms,
            read_at_ms: None,
            watch_authorization: Some(subscription.authorization.clone()),
        });
    }
    records
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::{RealmId, Role, WatchEventDetail, WatchEventKind, WatchEventMask};
    use std::collections::{HashMap, HashSet};
    use ulid::Ulid;

    const REALM: RealmId = RealmId([7u8; 32]);

    fn user(seed: u8) -> UserId {
        UserId::local(Ulid::from_bytes([seed; 16]), REALM)
    }

    fn group_doc_with_admins(assigned: HashSet<UserId>) -> GroupAuthorizationDocument {
        let mut doc = GroupAuthorizationDocument::new_default_group_doc(
            user(200),
            REALM,
            Ulid::from_bytes([9u8; 16]),
        );
        doc.roles
            .values_mut()
            .find(|role| role.name == "admin")
            .expect("default group admin role exists")
            .assigned_users = assigned;
        doc
    }

    #[test]
    fn group_admins_resolved_from_admin_roles() {
        let creator = user(1);
        let group_id = Ulid::from_bytes([2u8; 16]);
        let doc = GroupAuthorizationDocument::new_default_group_doc(creator, REALM, group_id);
        assert_eq!(group_admin_user_ids(&doc), vec![creator]);

        let (u2, u3, ignored) = (user(2), user(3), user(4));
        let mut doc = doc;
        doc.roles
            .values_mut()
            .find(|role| role.name == "admin")
            .expect("default group admin role exists")
            .assigned_users
            .extend([u2, u3]);
        let custom_role = Role {
            role_id: Ulid::r#gen(),
            name: "custom-admin-label".to_string(),
            permissions: HashMap::new(),
            assigned_users: HashSet::from([ignored]),
        };
        doc.roles.insert(custom_role.role_id, custom_role);

        let mut expected = vec![creator, u2, u3];
        expected.sort();
        assert_eq!(group_admin_user_ids(&doc), expected);
    }

    #[test]
    fn added_event_targets_affected_user_and_admins() {
        let (a1, a2, affected) = (user(10), user(11), user(20));
        let doc = group_doc_with_admins(HashSet::from([a1, a2]));
        let records = route_resource_event(
            &ResourceEvent::GroupMemberAdded {
                group_id: doc.group_id,
                affected_user: affected,
                actor_user_id: a1,
            },
            RoutingContext {
                group_auth: Some(&doc),
                realm_auth: None,
            },
            1_000,
        );

        assert_eq!(records.len(), 2);
        assert_eq!(records[0].recipient, affected);
        assert!(matches!(
            records[0].kind,
            NotificationKind::AddedToGroup { .. }
        ));
        assert_eq!(records[1].recipient, a2);
        assert!(matches!(
            records[1].kind,
            NotificationKind::GroupMemberAdded { .. }
        ));
        assert!(records.iter().all(|r| r.class == NotificationClass::Direct));
        let ids: HashSet<Ulid> = records.iter().map(|r| r.notification_id).collect();
        assert_eq!(ids.len(), records.len());
    }

    #[test]
    fn self_add_produces_admin_records_only() {
        let (a1, a2, actor) = (user(10), user(11), user(30));
        let doc = group_doc_with_admins(HashSet::from([a1, a2]));
        let records = route_resource_event(
            &ResourceEvent::GroupMemberAdded {
                group_id: doc.group_id,
                affected_user: actor,
                actor_user_id: actor,
            },
            RoutingContext {
                group_auth: Some(&doc),
                realm_auth: None,
            },
            1_000,
        );

        assert!(
            records
                .iter()
                .all(|r| matches!(r.kind, NotificationKind::GroupMemberAdded { .. }))
        );
        assert_eq!(records.len(), 2);
    }

    #[test]
    fn affected_admin_gets_membership_not_admin_record() {
        let (a1, actor) = (user(10), user(30));
        let doc = group_doc_with_admins(HashSet::from([a1]));
        let records = route_resource_event(
            &ResourceEvent::GroupMemberAdded {
                group_id: doc.group_id,
                affected_user: a1,
                actor_user_id: actor,
            },
            RoutingContext {
                group_auth: Some(&doc),
                realm_auth: None,
            },
            1_000,
        );

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].recipient, a1);
        assert!(matches!(
            records[0].kind,
            NotificationKind::AddedToGroup { .. }
        ));
    }

    #[test]
    fn removed_event_targets_affected_user_only() {
        let (affected, actor) = (user(20), user(30));
        let group_id = Ulid::from_bytes([9u8; 16]);
        let records = route_resource_event(
            &ResourceEvent::GroupMemberRemoved {
                group_id,
                affected_user: affected,
                actor_user_id: actor,
            },
            RoutingContext {
                group_auth: None,
                realm_auth: None,
            },
            1_000,
        );
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].recipient, affected);
        assert!(matches!(
            records[0].kind,
            NotificationKind::RemovedFromGroup { .. }
        ));

        let self_removal = route_resource_event(
            &ResourceEvent::GroupMemberRemoved {
                group_id,
                affected_user: actor,
                actor_user_id: actor,
            },
            RoutingContext {
                group_auth: None,
                realm_auth: None,
            },
            1_000,
        );
        assert!(self_removal.is_empty());
    }

    fn watch_subscription(owner: UserId, prefix: &str, mask: WatchEventMask) -> WatchSubscription {
        WatchSubscription::new(owner, prefix.to_string(), mask, 1_000)
    }

    fn upload_event(actor: UserId, path: &str) -> WatchEvent {
        let resource = aruna_core::structs::parse_data_watch_resource_path(path)
            .expect("canonical data watch path");
        WatchEvent {
            event_id: Ulid::from_bytes([7u8; 16]),
            realm_id: REALM,
            kind: WatchEventKind::DataUploaded,
            path: path.to_string(),
            actor,
            occurred_at_ms: 5_000,
            detail: WatchEventDetail::DataUploaded {
                group_id: resource.group_id,
                node_id: resource.node_id,
                bucket: resource.bucket.to_string(),
                key: resource.key_prefix.to_string(),
                size_bytes: 10,
            },
        }
    }

    fn data_path(node_id: aruna_core::NodeId, bucket: &str, key: &str) -> String {
        aruna_core::structs::data_watch_resource_path(
            Ulid::from_bytes([6u8; 16]),
            node_id,
            bucket,
            key,
        )
    }

    fn metadata_event(actor: UserId, group_id: Ulid, document_id: MetaResourceId) -> WatchEvent {
        WatchEvent {
            event_id: Ulid::from_bytes([8u8; 16]),
            realm_id: REALM,
            kind: WatchEventKind::MetadataCreated,
            path: format!("meta/{group_id}/datasets/project/runs/run-42"),
            actor,
            occurred_at_ms: 5_000,
            detail: WatchEventDetail::MetadataCreated {
                group_id,
                document_id,
            },
        }
    }

    #[test]
    fn watch_event_matches_prefix_mask_and_skips_self() {
        let owner = user(1);
        let actor = user(2);
        let data_node = iroh::SecretKey::from_bytes(&[6u8; 32]).public();
        let subs = vec![
            // Matches: prefix + mask.
            watch_subscription(
                owner,
                &data_path(data_node, "bucket", ""),
                WatchEventMask::from_kinds([WatchEventKind::DataUploaded]),
            ),
            // Prefix mismatch.
            watch_subscription(
                owner,
                &data_path(data_node, "other", ""),
                WatchEventMask::from_kinds([WatchEventKind::DataUploaded]),
            ),
            // Mask mismatch.
            watch_subscription(
                owner,
                &data_path(data_node, "bucket", ""),
                WatchEventMask::from_kinds([WatchEventKind::MetadataCreated]),
            ),
            // Self-notify: owner is the actor.
            watch_subscription(
                actor,
                &data_path(data_node, "bucket", ""),
                WatchEventMask::from_kinds([WatchEventKind::DataUploaded]),
            ),
        ];

        let records = route_watch_event(
            &upload_event(actor, &data_path(data_node, "bucket", "object")),
            &subs,
        );
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].recipient, owner);
        assert_eq!(records[0].class, NotificationClass::Transient);
        assert_eq!(records[0].created_at_ms, 5_000);
        assert_eq!(
            records[0]
                .watch_authorization
                .as_ref()
                .unwrap()
                .watch_path_prefix,
            subs[0].path_prefix
        );
        assert!(matches!(
            records[0].kind,
            NotificationKind::DataUploaded { .. }
        ));
    }

    #[test]
    fn nested_metadata_document_path_matches_canonical_prefix() {
        let owner = user(1);
        let actor = user(2);
        let group_id = Ulid::from_bytes([3u8; 16]);
        let document_id = Ulid::from_bytes([4u8; 16]);
        let subscription = watch_subscription(
            owner,
            &format!("meta/{group_id}/datasets/project"),
            WatchEventMask::from_kinds([WatchEventKind::MetadataCreated]),
        );

        let records = route_watch_event(
            &metadata_event(actor, group_id, document_id),
            &[subscription],
        );

        assert_eq!(records.len(), 1);
        assert!(matches!(
            &records[0].kind,
            NotificationKind::MetadataCreated {
                path,
                group_id: event_group_id,
                document_id: event_document_id,
                ..
            } if path == &format!("meta/{group_id}/datasets/project/runs/run-42")
                && event_group_id == &group_id
                && event_document_id == &document_id
        ));
    }

    #[test]
    fn watch_event_ids_are_deterministic_across_invocations() {
        let owner = user(1);
        let actor = user(2);
        let data_node = iroh::SecretKey::from_bytes(&[6u8; 32]).public();
        let subs = vec![watch_subscription(
            owner,
            &data_path(data_node, "bucket", ""),
            WatchEventMask::from_kinds([WatchEventKind::DataUploaded]),
        )];
        let event = upload_event(actor, &data_path(data_node, "bucket", "object"));

        let first = route_watch_event(&event, &subs);
        let second = route_watch_event(&event, &subs);
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].notification_id, second[0].notification_id);
    }

    #[test]
    fn watch_event_with_no_matches_is_empty() {
        let actor = user(2);
        let data_node = iroh::SecretKey::from_bytes(&[6u8; 32]).public();
        assert!(
            route_watch_event(
                &upload_event(actor, &data_path(data_node, "bucket", "object")),
                &[]
            )
            .is_empty()
        );
    }

    #[test]
    fn delayed_watch_event_skips_subscriptions_created_after_it_occurred() {
        let owner = user(1);
        let actor = user(2);
        let data_node = iroh::SecretKey::from_bytes(&[6u8; 32]).public();
        let mask = WatchEventMask::from_kinds([WatchEventKind::DataUploaded]);
        let prefix = data_path(data_node, "bucket", "");
        let mut existing = watch_subscription(owner, &prefix, mask);
        existing.created_at_ms = 5_000;
        let mut retroactive = watch_subscription(user(3), &prefix, mask);
        retroactive.created_at_ms = 5_001;

        let records = route_watch_event(
            &upload_event(actor, &data_path(data_node, "bucket", "object")),
            &[existing, retroactive],
        );

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].recipient, owner);
    }

    #[test]
    fn watch_data_prefix_is_node_disambiguated() {
        let owner = user(1);
        let actor = user(2);
        let mask = WatchEventMask::from_kinds([WatchEventKind::DataUploaded]);
        let watched_node = iroh::SecretKey::from_bytes(&[6u8; 32]).public();
        let other_node = iroh::SecretKey::from_bytes(&[7u8; 32]).public();
        let scoped = vec![watch_subscription(
            owner,
            &data_path(watched_node, "reports", ""),
            mask,
        )];

        assert!(
            route_watch_event(
                &upload_event(actor, &data_path(other_node, "reports", "x")),
                &scoped
            )
            .is_empty()
        );
        assert_eq!(
            route_watch_event(
                &upload_event(actor, &data_path(watched_node, "reports", "x")),
                &scoped
            )
            .len(),
            1
        );
    }
}
