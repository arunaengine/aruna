//! Loads request policies for async callers and enforces them on one request.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::request_policy::PolicyRequest;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::types::GroupId;

use crate::auth::enforcement::{
    PolicyBulkOperation, PolicyEvaluator, PolicyEvaluators, PolicyLoadOperation,
};
use crate::auth::error::PolicyEnforcementError;
use crate::driver::{DriverContext, drive};

impl PolicyEvaluator {
    /// Reads and compiles the realm's, and optionally one group's, policy set.
    /// Missing policy state, a read failure, and a compile failure all fail
    /// closed; the group scope is skipped only when the request names no group.
    pub async fn load(
        context: &DriverContext,
        realm_id: RealmId,
        group_id: Option<GroupId>,
    ) -> Result<Self, PolicyEnforcementError> {
        drive(PolicyLoadOperation::new(realm_id, group_id), context).await
    }

    /// Loads one evaluator per distinct realm/group scope.
    pub async fn load_bulk(
        context: &DriverContext,
        groups: impl IntoIterator<Item = (RealmId, GroupId)>,
    ) -> Result<PolicyEvaluators, PolicyEnforcementError> {
        drive(PolicyBulkOperation::new(groups)?, context).await
    }
}

/// Evaluates realm then group policies for one request. Missing state and read,
/// compile, or evaluation failures deny. Either scope may deny; neither may
/// grant.
pub async fn enforce_policies(
    context: &DriverContext,
    realm_id: RealmId,
    request: &PolicyRequest,
) -> Result<(), PolicyEnforcementError> {
    drive(PolicyLoadOperation::for_request(realm_id, request), context)
        .await?
        .evaluate(request)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ulid::Ulid;

    fn context_at(directory: &tempfile::TempDir) -> DriverContext {
        DriverContext {
            storage_handle: aruna_storage::FjallStorage::open(
                directory.path().to_str().expect("storage path"),
            )
            .expect("storage opens"),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            outbound_handle: None,
            task_handle: None,
            compute_handle: None,
        }
    }

    async fn seed_realm(context: &DriverContext, realm_id: RealmId) {
        let actor = aruna_core::structs::identity::auth::Actor {
            node_id: iroh::SecretKey::from_bytes(&[3u8; 32]).public(),
            user_id: aruna_core::UserId::nil(realm_id),
            realm_id,
        };
        let config = aruna_core::structs::identity::realm::RealmConfigDocument::default_for_realm(
            realm_id,
            Vec::new(),
        );
        let event = context
            .storage_handle
            .send_storage_effect(aruna_core::effects::StorageEffect::Write {
                key_space: aruna_core::keyspaces::REALM_CONFIG_KEYSPACE.to_string(),
                key: aruna_core::types::Key::from(*realm_id.as_bytes()),
                value: config.to_bytes(&actor).expect("config encodes").into(),
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            aruna_core::events::Event::Storage(
                aruna_core::events::StorageEvent::WriteResult { .. }
            )
        ));
    }

    #[tokio::test]
    async fn missing_is_unavailable() {
        let directory = tempfile::tempdir().unwrap();
        let context = context_at(&directory);
        let result = PolicyEvaluator::load_bulk(
            &context,
            [(RealmId([5u8; 32]), Ulid::from_bytes([8u8; 16]))],
        )
        .await;

        assert!(matches!(
            result,
            Err(PolicyEnforcementError::Unavailable(message))
                if message == "realm policy state is unavailable"
        ));
    }

    #[tokio::test]
    async fn missing_state_denies() {
        // Unreadable realm or group state must deny; only a request that names no
        // group may skip the group scope.
        let directory = tempfile::tempdir().unwrap();
        let context = context_at(&directory);
        let realm_id = RealmId([6u8; 32]);
        let group_id = Ulid::from_bytes([8u8; 16]);

        assert!(matches!(
            PolicyEvaluator::load(&context, realm_id, None).await,
            Err(PolicyEnforcementError::Unavailable(message))
                if message == "realm policy state is unavailable"
        ));

        seed_realm(&context, realm_id).await;

        assert!(matches!(
            PolicyEvaluator::load(&context, realm_id, Some(group_id)).await,
            Err(PolicyEnforcementError::Unavailable(message))
                if message == "group policy state is unavailable"
        ));
        let request = PolicyRequest::basic("/r".to_string(), "read".to_string(), String::new());
        assert!(
            PolicyEvaluator::load(&context, realm_id, None)
                .await
                .and_then(|evaluator| evaluator.evaluate(&request))
                .is_ok()
        );
    }
}
