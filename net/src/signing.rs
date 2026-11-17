//! Signs policy publications and vault records with this node's key, refusing any claim or
//! record that names another node.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::events::{PolicySignEvent, VaultSignEvent};
use aruna_core::structs::identity::user::vault::VaultRecords;
use aruna_core::structs::placement::policy::document::PolicyPublicationClaim;

/// Signs one publication claim. A claim naming another publisher is refused, so no operation can
/// mint provenance for a peer.
pub(crate) fn sign_publication(
    key: &iroh::SecretKey,
    claim: PolicyPublicationClaim,
) -> PolicySignEvent {
    if claim.publisher != key.public() {
        return PolicySignEvent::Unavailable("claim names another publisher".to_string());
    }
    PolicySignEvent::Signed(Box::new(claim.sign(key)))
}

/// Signs records this node publishes. A record naming another node stays unsigned and fails the
/// whole batch, so no operation can sign for another origin.
pub(crate) fn sign_records(key: &iroh::SecretKey, mut records: VaultRecords) -> VaultSignEvent {
    let node_id = key.public();
    let signed = match &mut records {
        VaultRecords::Heads(heads) => heads.iter_mut().try_for_each(|head| {
            (head.node_id == node_id).then_some(())?;
            head.signature = Some(key.sign(&head.signing_bytes().ok()?));
            Some(())
        }),
        VaultRecords::Keys(keys) => keys.iter_mut().try_for_each(|key_record| {
            (key_record.node_id == node_id).then_some(())?;
            key_record.signature = Some(key.sign(&key_record.signing_bytes().ok()?));
            Some(())
        }),
    };
    match signed {
        Some(()) => VaultSignEvent::Signed(Box::new(records)),
        None => VaultSignEvent::Unavailable("record names another publisher".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::UserId;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::identity::user::vault::VaultRevision;
    use aruna_core::structs::placement::policy::PlacementPolicyRef;
    use aruna_core::structs::placement::record::PlacementRef;
    use ulid::Ulid;

    fn realm() -> RealmId {
        RealmId::from_bytes([3u8; 32])
    }

    fn claim(publisher: iroh::PublicKey) -> PolicyPublicationClaim {
        PolicyPublicationClaim {
            realm_id: realm(),
            policy_ref: PlacementPolicyRef {
                policy_id: Ulid::from_parts(1, 1),
                digest: [5u8; 32],
            },
            publisher,
            created_by: UserId::nil(realm()),
            created_at_ms: 7,
            event_id: Ulid::from_parts(2, 2),
            config_digest: [4u8; 32],
        }
    }

    fn head(node_id: iroh::PublicKey) -> VaultRevision {
        VaultRevision {
            realm_id: realm(),
            user_id: UserId::nil(realm()),
            revision_id: Ulid::from_parts(3, 3),
            predecessors: Vec::new(),
            payload: None,
            node_id,
            placement: PlacementRef::NIL,
            created_at_ms: 9,
            signature: None,
        }
    }

    #[test]
    fn signs_own_claim() {
        let key = iroh::SecretKey::from_bytes(&[1u8; 32]);
        let claim = claim(key.public());
        let PolicySignEvent::Signed(publication) = sign_publication(&key, claim.clone()) else {
            panic!("own claim must be signed");
        };
        assert_eq!(*publication, claim.sign(&key));
    }

    #[test]
    fn refuses_foreign_claim() {
        let key = iroh::SecretKey::from_bytes(&[1u8; 32]);
        let other = iroh::SecretKey::from_bytes(&[2u8; 32]).public();
        assert!(matches!(
            sign_publication(&key, claim(other)),
            PolicySignEvent::Unavailable(_)
        ));
    }

    #[test]
    fn signs_own_records() {
        let key = iroh::SecretKey::from_bytes(&[1u8; 32]);
        let records = VaultRecords::Heads(vec![head(key.public())]);
        let VaultSignEvent::Signed(signed) = sign_records(&key, records) else {
            panic!("own records must be signed");
        };
        let VaultRecords::Heads(heads) = *signed else {
            panic!("heads stay heads");
        };
        let bytes = heads[0].signing_bytes().unwrap();
        assert_eq!(heads[0].signature, Some(key.sign(&bytes)));
    }

    #[test]
    fn refuses_foreign_records() {
        let key = iroh::SecretKey::from_bytes(&[1u8; 32]);
        let other = iroh::SecretKey::from_bytes(&[2u8; 32]).public();
        let records = VaultRecords::Heads(vec![head(key.public()), head(other)]);
        assert!(matches!(
            sign_records(&key, records),
            VaultSignEvent::Unavailable(_)
        ));
    }
}
