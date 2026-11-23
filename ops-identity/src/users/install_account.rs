//! Reads the account a plugin install runs as, and decides whether it is that install's own
//! service account.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult};
use aruna_core::keyspaces::USER_KEYSPACE;
use aruna_core::rows::values;
use aruna_core::structs::identity::user::User;
use aruna_core::types::{GroupId, Key};
use aruna_core::user::id::UserId;
use ulid::Ulid;

/// The call that reads an account, for an operation that decides with it.
pub fn read_user_call(user_id: UserId) -> IoCall {
    let key = Key::from(user_id.to_bytes().to_vec());
    IoCall::Read(vec![(USER_KEYSPACE.to_string(), key)])
}

/// The account a [`read_user_call`] answered; an unreadable row is a codec error.
pub fn read_user_result(result: IoResult) -> Result<Option<User>, IoError> {
    let [row] = values::<1>(result)?;
    (row.map(|bytes| User::from_bytes(&bytes)).transpose())
        .map_err(|error| IoError::Codec(error.to_string()))
}

/// Whether `user` is the service account of install `install_id` in group `group_id`.
pub fn owns_install(user: &User, group_id: GroupId, install_id: Ulid) -> bool {
    user.service_group() == Some(group_id) && user.install() == Some(install_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    // The read names the account row; a missing row is no account and a broken one an error.
    #[test]
    fn reads_account_row() {
        let realm_id = aruna_core::structs::identity::realm::RealmId([1; 32]);
        let user_id = UserId::local(Ulid::from_bytes([3; 16]), realm_id);
        let IoCall::Read(rows) = read_user_call(user_id) else {
            panic!("an account is read");
        };
        assert_eq!(rows[0].0, USER_KEYSPACE);
        let missing = read_user_result(IoResult::Values(vec![None]));
        assert!(matches!(missing, Ok(None)));
        let broken = IoResult::Values(vec![Some(aruna_core::types::Value::from(&b"x"[..]))]);
        assert!(matches!(read_user_result(broken), Err(IoError::Codec(_))));
    }
}
