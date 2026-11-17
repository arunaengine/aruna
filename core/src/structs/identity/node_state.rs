//! The persisted identity of a node: how it booted, its onboarding state and its keys.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde::{Deserialize, Serialize};

use crate::UserId;
use crate::onboarding::OnboardingPhase;
use crate::structs::identity::realm::RealmId;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BootOrigin {
    InitializedRealm,
    Onboarded,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PersistedNodeStatus {
    PendingInitialization,
    PendingOnboarding,
    Complete,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PersistedNodeIdentity {
    Management {
        #[serde(rename = "realm_private_key_pem")]
        realm_private_pem: String,
    },
    Server {
        #[serde(rename = "issuer_private_key_pem")]
        private_key_pem: String,
        delegation_signature: String,
    },
    /// Owner-bound device. The owner is copied from the enrollment answer: a
    /// device holds no realm state to read it back from before it has joined.
    User { owner: UserId },
}

impl std::fmt::Debug for PersistedNodeIdentity {
    /// Redacts private key material: this record may be printed in a diagnostic
    /// path, and keys are not diagnostic output.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Management { .. } => f
                .debug_struct("Management")
                .field("realm_private_key_pem", &"<redacted>")
                .finish(),
            Self::Server { .. } => f
                .debug_struct("Server")
                .field("issuer_private_key_pem", &"<redacted>")
                .field("delegation_signature", &"<redacted>")
                .finish(),
            Self::User { owner } => f.debug_struct("User").field("owner", owner).finish(),
        }
    }
}

impl PersistedNodeIdentity {
    /// Owner of a device identity; `None` for infrastructure nodes.
    pub fn owner(&self) -> Option<UserId> {
        match self {
            Self::User { owner } => Some(*owner),
            Self::Management { .. } | Self::Server { .. } => None,
        }
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedNodeState {
    pub boot_origin: BootOrigin,
    pub status: PersistedNodeStatus,
    pub realm_id: RealmId,
    pub net_secret_key: [u8; 32],
    pub onboarding_phase: Option<OnboardingPhase>,
    pub onboarding_sync_ticket: Option<String>,
    pub identity: PersistedNodeIdentity,
}

impl std::fmt::Debug for PersistedNodeState {
    /// Redacts the network secret; the rest of the record is diagnostic.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PersistedNodeState")
            .field("boot_origin", &self.boot_origin)
            .field("status", &self.status)
            .field("realm_id", &self.realm_id)
            .field("net_secret_key", &"<redacted>")
            .field("onboarding_phase", &self.onboarding_phase)
            .field("onboarding_sync_ticket", &self.onboarding_sync_ticket)
            .field("identity", &self.identity)
            .finish()
    }
}
