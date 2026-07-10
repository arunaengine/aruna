use aruna_core::structs::{
    GroupAuthorizationDocument, NotificationClass, NotificationKind, NotificationRecord,
    RealmAuthorizationDocument, ResourceEvent,
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

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::{RealmId, Role};
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
            role_id: Ulid::new(),
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
}
