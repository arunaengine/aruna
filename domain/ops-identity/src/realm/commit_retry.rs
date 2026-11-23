//! Runs realm config setters' write transactions in unit tests and checks their conflict reruns.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::keyspaces::REALM_CONFIG_KEYSPACE;
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::structs::identity::auth::Actor;
use aruna_core::structs::identity::realm::RealmConfigDocument;

use super::COMMIT_ATTEMPTS;

/// Checks that a setter's write transaction reruns from its read after a lost commit.
pub(super) fn retries_conflicts<O: Operation>() {
    assert!(O::is_conflict(&O::Error::from(IoError::Conflict)));
    assert_eq!(O::conflict_retries(), COMMIT_ATTEMPTS as u32 - 1);
}

/// Runs a setter's write transaction once on `config` with no admin state yet, and returns the
/// config it writes and its output, which comes only with the drain wake after the commit.
pub(super) fn write_once<O: Operation>(
    write: &mut O,
    actor: &Actor,
    config: &RealmConfigDocument,
) -> (RealmConfigDocument, O::Output) {
    assert_eq!(write.transaction(), TransactionMode::Write);
    assert!(matches!(write.first(), Ok(Next::Call(IoCall::Read(rows))) if rows.len() == 2));
    let read = IoResult::Values(vec![Some(config.to_bytes(actor).unwrap().into()), None]);
    let mut turn = write.next(read);
    let mut written = None;
    loop {
        turn = match turn {
            Ok(Next::Call(IoCall::Write(rows))) => {
                let (_, _, value) = rows
                    .iter()
                    .find(|(space, _, _)| space == REALM_CONFIG_KEYSPACE)
                    .expect("the write stores the realm config");
                written = Some(RealmConfigDocument::from_bytes(value).unwrap());
                write.next(IoResult::Done)
            }
            Ok(Next::Call(IoCall::Delete(_))) => write.next(IoResult::Done),
            Ok(Next::Then { output, calls }) => {
                assert!(matches!(calls.as_slice(), [IoCall::Effect(_)]));
                return (written.expect("the config is written"), output);
            }
            Ok(_) => panic!("the write asked for another call before its commit"),
            Err(error) => panic!("the write failed: {error:?}"),
        };
    }
}
