//! Creates a session at the session limit by signing out the user's oldest sessions first.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::create::{
    CreateSessionConfig, CreateSessionError, CreateSessionOperation, CreatedSession,
};
use crate::driver::drive;
use crate::auth::bearer_token::UserCutoffOperation;

/// Creates a session for the user. At the session limit the user's cutoff moves past the
/// oldest active sessions, which signs them out, and creation runs again. A service account
/// keeps the plain limit, so renewing never ends the sessions its automation uses.
pub async fn create_making_room(
    context: &crate::driver::DriverContext,
    node_id: aruna_core::id::NodeId,
    mut config: CreateSessionConfig,
) -> Result<CreatedSession, CreateSessionError> {
    let read_cutoff = |realm_id, user_id| async move {
        drive(UserCutoffOperation::new(realm_id, &user_id), &context)
            .await
            .map_err(|error| CreateSessionError::Retire(error.to_string()))
    };
    let now = config.time;
    config.cutoff = read_cutoff(config.realm_id, config.user_id).await?;
    // A token issued before the cutoff would be refused at once, so it is issued at it.
    let at_cutoff = |config: &mut CreateSessionConfig| {
        config.time = config.time.max(config.cutoff.unwrap_or(0));
    };
    at_cutoff(&mut config);
    loop {
        let oldest = match crate::driver::drive(
            CreateSessionOperation::new(config.clone()),
            context,
        )
        .await
        {
            Err(CreateSessionError::LimitReached { oldest }) => oldest,
            other => return other,
        };
        let read = crate::users::read_document::ReadUserOperation::new(config.user_id);
        let service = match crate::driver::drive(read, context).await {
            Ok(user) => user.service_group().is_some(),
            // An unknown account may be a service account not yet replicated here.
            Err(crate::users::read_document::ReadUserError::NotFound) => true,
            Err(error) => return Err(CreateSessionError::Retire(error.to_string())),
        };
        if service {
            return Err(CreateSessionError::LimitReached { oldest });
        }
        let actor = aruna_core::structs::identity::auth::Actor {
            node_id,
            user_id: config.user_id,
            realm_id: config.realm_id,
        };
        let retire = super::revoke::cutoff_revocation(actor, oldest, now);
        crate::driver::drive(
            crate::auth::revoke_token::RevokeTokenOperation::new(retire),
            context,
        )
        .await
        .map_err(|error| CreateSessionError::Retire(error.to_string()))?;
        let cutoff = read_cutoff(config.realm_id, config.user_id).await?;
        // Without progress the limit stands rather than looping.
        if cutoff.is_none_or(|cutoff| cutoff <= oldest) {
            return Err(CreateSessionError::LimitReached { oldest });
        }
        config.cutoff = cutoff;
        at_cutoff(&mut config);
    }
}

#[cfg(test)]
#[path = "limit_tests.rs"]
mod tests;
