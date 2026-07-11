use aruna_core::NodeId;
use aruna_core::errors::ConversionError;
use aruna_core::structs::{RealmConfigDocument, WatchSubscription};
use aruna_core::types::{GroupId, UserId};

use crate::sync_placement::select_topic_peers;

pub const NOTIFICATION_INBOX_TOPIC_DOMAIN: &[u8] = b"aruna-notification-inbox-v1";
pub const QUOTA_STATE_OWNER_TOPIC_DOMAIN: &[u8] = b"aruna-quota-state-owner-v1";

pub fn quota_state_topic_id(group_id: &GroupId) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(QUOTA_STATE_OWNER_TOPIC_DOMAIN);
    hasher.update(&group_id.to_bytes());
    *hasher.finalize().as_bytes()
}

/// Elects the single node responsible for evaluating a group's quota-state
/// transitions and emitting notifications, so cross-node sums do not duplicate
/// them. Mirrors [`resolve_inbox_holder`]; R stays 1 until the placement map.
pub fn resolve_quota_state_owner(
    group_id: &GroupId,
    realm_config: &RealmConfigDocument,
) -> Result<Option<NodeId>, ConversionError> {
    let candidates = realm_config.sync_eligible_node_ids()?;
    Ok(
        select_topic_peers(&quota_state_topic_id(group_id), &candidates, &[], 1)
            .into_iter()
            .next(),
    )
}

pub fn inbox_topic_id(user_id: &UserId) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(NOTIFICATION_INBOX_TOPIC_DOMAIN);
    hasher.update(&user_id.to_storage_key());
    *hasher.finalize().as_bytes()
}

pub fn resolve_inbox_holder(
    user_id: &UserId,
    realm_config: &RealmConfigDocument,
) -> Result<Option<NodeId>, ConversionError> {
    // Single swap point for the #261/#264 placement map; R stays 1 until then.
    let candidates = realm_config.sync_eligible_node_ids()?;
    Ok(
        select_topic_peers(&inbox_topic_id(user_id), &candidates, &[], 1)
            .into_iter()
            .next(),
    )
}

pub fn filter_locally_held_watch_subscriptions(
    subscriptions: Vec<WatchSubscription>,
    realm_config: &RealmConfigDocument,
    local_node_id: NodeId,
) -> Result<(Vec<WatchSubscription>, bool), ConversionError> {
    let mut locally_held = Vec::with_capacity(subscriptions.len());
    let mut found_stale = false;
    for subscription in subscriptions {
        if resolve_inbox_holder(&subscription.owner, realm_config)? == Some(local_node_id) {
            locally_held.push(subscription);
        } else {
            found_stale = true;
        }
    }
    Ok((locally_held, found_stale))
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::{RealmId, RealmNodeKind};
    use std::collections::HashSet;
    use ulid::Ulid;

    fn node(seed: u8) -> NodeId {
        let mut bytes = [0u8; 32];
        bytes[0] = seed;
        iroh::SecretKey::from_bytes(&bytes).public()
    }

    fn user(seed: u8) -> UserId {
        let mut ulid_bytes = [0u8; 16];
        ulid_bytes[0] = seed;
        UserId::new(Ulid::from_bytes(ulid_bytes), RealmId::from_bytes([1u8; 32]))
    }

    fn config_with(nodes: &[(NodeId, RealmNodeKind)]) -> RealmConfigDocument {
        let mut config = RealmConfigDocument::new(RealmId::from_bytes([1u8; 32]), Vec::new(), 3);
        for (node_id, kind) in nodes {
            config.ensure_node(*node_id, kind.clone());
        }
        config
    }

    #[test]
    fn holder_is_deterministic() {
        let u = user(1);
        let config_a = config_with(&[
            (node(1), RealmNodeKind::Server),
            (node(2), RealmNodeKind::Server),
            (node(3), RealmNodeKind::Server),
            (node(4), RealmNodeKind::Server),
        ]);
        let config_b = config_with(&[
            (node(3), RealmNodeKind::Server),
            (node(1), RealmNodeKind::Server),
            (node(4), RealmNodeKind::Server),
            (node(2), RealmNodeKind::Server),
        ]);

        let holder = resolve_inbox_holder(&u, &config_a).unwrap();
        assert!(holder.is_some());
        assert_eq!(holder, resolve_inbox_holder(&u, &config_a).unwrap());
        assert_eq!(holder, resolve_inbox_holder(&u, &config_b).unwrap());
    }

    #[test]
    fn holder_is_uniform_across_resolving_nodes() {
        let u = user(5);
        let config = config_with(&[
            (node(1), RealmNodeKind::Server),
            (node(2), RealmNodeKind::Server),
            (node(7), RealmNodeKind::Server),
            (node(8), RealmNodeKind::Server),
        ]);

        assert_eq!(
            resolve_inbox_holder(&u, &config).unwrap(),
            resolve_inbox_holder(&u, &config).unwrap()
        );
    }

    #[test]
    fn different_users_can_map_to_different_holders() {
        let config = config_with(&[
            (node(1), RealmNodeKind::Server),
            (node(2), RealmNodeKind::Server),
            (node(3), RealmNodeKind::Server),
            (node(4), RealmNodeKind::Server),
        ]);

        let mut holders = HashSet::new();
        for seed in 0..16u8 {
            if let Some(holder) = resolve_inbox_holder(&user(seed), &config).unwrap() {
                holders.insert(holder.as_bytes().to_vec());
            }
        }

        assert!(holders.len() >= 2);
    }

    #[test]
    fn user_nodes_are_never_holders() {
        let server = node(1);
        let user_node = node(2);
        let config = config_with(&[
            (server, RealmNodeKind::Server),
            (user_node, RealmNodeKind::User),
        ]);

        for seed in 0..32u8 {
            let holder = resolve_inbox_holder(&user(seed), &config).unwrap();
            assert_ne!(holder, Some(user_node));
            assert_eq!(holder, Some(server));
        }
    }

    #[test]
    fn holder_re_ranks_when_eligible_set_changes() {
        let all = [
            (node(1), RealmNodeKind::Server),
            (node(2), RealmNodeKind::Server),
            (node(3), RealmNodeKind::Server),
            (node(4), RealmNodeKind::Server),
        ];
        let u = user(9);
        let full = config_with(&all);
        let holder = resolve_inbox_holder(&u, &full).unwrap().unwrap();

        let without_holder: Vec<_> = all
            .iter()
            .filter(|(node_id, _)| *node_id != holder)
            .cloned()
            .collect();
        let reduced = config_with(&without_holder);
        let new_holder = resolve_inbox_holder(&u, &reduced).unwrap().unwrap();
        assert_ne!(new_holder, holder);

        let restored = config_with(&all);
        assert_eq!(
            resolve_inbox_holder(&u, &restored).unwrap().unwrap(),
            holder
        );
    }

    #[test]
    fn empty_eligible_set_yields_none() {
        let only_users = config_with(&[
            (node(1), RealmNodeKind::User),
            (node(2), RealmNodeKind::User),
        ]);
        assert_eq!(resolve_inbox_holder(&user(1), &only_users).unwrap(), None);

        let empty = config_with(&[]);
        assert_eq!(resolve_inbox_holder(&user(1), &empty).unwrap(), None);
    }

    #[test]
    fn topic_id_is_domain_separated() {
        let u = user(1);
        let other = user(2);

        let plain = *blake3::hash(&u.to_storage_key()).as_bytes();
        assert_ne!(inbox_topic_id(&u), plain);
        assert_ne!(inbox_topic_id(&u), inbox_topic_id(&other));
    }
}
