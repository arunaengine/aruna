//! Creates a session at the session limit by signing out the user's oldest sessions first.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::id::NodeId;
use aruna_core::operation::Operation;
use aruna_core::structs::identity::auth::Actor;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::identity::user::User;

use super::create::{
    CreateSessionConfig, CreateSessionError, CreateSessionOperation, CreatedSession,
};
use super::revoke::cutoff_revocation;
use crate::auth::bearer_token::UserCutoffOperation;
use crate::auth::error::ArunaBearerError;
use crate::auth::revoke_token::{RevokeTokenError, RevokeTokenOperation};
use crate::users::read_document::{ReadUserError, ReadUserOperation};

#[derive(Debug, PartialEq)]
enum Step {
    Cutoff,
    Create,
    Account { oldest: u64 },
    Retire { oldest: u64 },
    Recheck { oldest: u64 },
}

type RoomTurn = Result<Next<CreatedSession>, CreateSessionError>;

fn retire_failed(error: impl std::fmt::Display) -> CreateSessionError {
    CreateSessionError::Retire(error.to_string())
}

/// Creates a session for the user. At the session limit the user's cutoff moves past the
/// oldest active sessions, which signs them out, and creation runs again. A service account
/// keeps the plain limit, so renewing never ends the sessions its automation uses.
#[derive(Debug, PartialEq)]
pub struct CreateMakingRoomOperation {
    node_id: NodeId,
    config: CreateSessionConfig,
    now: u64,
    step: Step,
}

impl CreateMakingRoomOperation {
    pub fn new(node_id: NodeId, config: CreateSessionConfig) -> Self {
        Self {
            node_id,
            now: config.time,
            config,
            step: Step::Cutoff,
        }
    }

    fn read_cutoff(&self) -> IoCall {
        let (realm_id, user_id) = (self.config.realm_id, self.config.user_id);
        IoCall::child(UserCutoffOperation::new(realm_id, &user_id))
    }

    /// A token issued before the cutoff would be refused at once, so it is issued at it.
    fn create(&mut self, cutoff: Option<u64>) -> RoomTurn {
        self.config.cutoff = cutoff;
        self.config.time = self.config.time.max(cutoff.unwrap_or(0));
        self.step = Step::Create;
        let create = CreateSessionOperation::new(self.config.clone());
        Ok(Next::Call(IoCall::child(create)))
    }

    fn retire(&mut self, oldest: u64) -> RoomTurn {
        let actor = Actor {
            node_id: self.node_id,
            user_id: self.config.user_id,
            realm_id: self.config.realm_id,
        };
        self.step = Step::Retire { oldest };
        let retire = cutoff_revocation(actor, oldest, self.now);
        Ok(Next::Call(IoCall::child(RevokeTokenOperation::new(retire))))
    }
}

impl Operation for CreateMakingRoomOperation {
    type Output = CreatedSession;
    type Error = CreateSessionError;

    fn first(&mut self) -> RoomTurn {
        Ok(Next::Call(self.read_cutoff()))
    }

    fn next(&mut self, result: IoResult) -> RoomTurn {
        match self.step {
            Step::Cutoff => {
                let cutoff = result.child::<Option<u64>, ArunaBearerError>()?;
                self.create(cutoff.map_err(retire_failed)?)
            }
            Step::Create => match result.child::<CreatedSession, CreateSessionError>()? {
                Err(CreateSessionError::LimitReached { oldest }) => {
                    self.step = Step::Account { oldest };
                    let read = ReadUserOperation::new(self.config.user_id);
                    Ok(Next::Call(IoCall::child(read)))
                }
                other => Ok(Next::Done(other?)),
            },
            Step::Account { oldest } => {
                let service = match result.child::<User, ReadUserError>()? {
                    Ok(user) => user.service_group().is_some(),
                    // An unknown account may be a service account not yet replicated here.
                    Err(ReadUserError::NotFound) => true,
                    Err(error) => return Err(retire_failed(error)),
                };
                match service {
                    true => Err(CreateSessionError::LimitReached { oldest }),
                    false => self.retire(oldest),
                }
            }
            Step::Retire { oldest } => {
                let retired = result.child::<RealmConfigDocument, RevokeTokenError>()?;
                retired.map_err(retire_failed)?;
                self.step = Step::Recheck { oldest };
                Ok(Next::Call(self.read_cutoff()))
            }
            Step::Recheck { oldest } => {
                let cutoff = result.child::<Option<u64>, ArunaBearerError>()?;
                let cutoff = cutoff.map_err(retire_failed)?;
                // Without progress the limit stands rather than looping.
                if cutoff.is_none_or(|cutoff| cutoff <= oldest) {
                    return Err(CreateSessionError::LimitReached { oldest });
                }
                self.create(cutoff)
            }
        }
    }
}

#[cfg(test)]
#[path = "limit_tests.rs"]
mod tests;
