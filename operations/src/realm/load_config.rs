//! Loads the realm config document for callers that fall back when it is unavailable.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use tracing::warn;

use crate::document_repository::GetConfigOperation;
use crate::driver::{DriverContext, drive};

/// The realm config of `realm_id`, or `None` with a warning when it cannot be read.
pub async fn load_realm_config(
    context: &DriverContext,
    realm_id: RealmId,
) -> Option<RealmConfigDocument> {
    match drive(GetConfigOperation::new(realm_id), context).await {
        Ok(config) => Some(config),
        Err(error) => {
            warn!(error = %error, "realm config unavailable; querying the local replica only");
            None
        }
    }
}
