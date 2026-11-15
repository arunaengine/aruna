//! Small private values of the install as JSON, changed by compare and swap: a write uses the
//! version this run read, or makes a new value when the run did not read the key.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::cell::RefCell;
use std::collections::BTreeMap;

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::host;
use crate::{Error, Result};

/// How often `update` reads and writes again after a conflict.
const TRIES: usize = 5;

thread_local! {
    /// The version of each key this run read or wrote; `None` when it read no value.
    static READ: RefCell<BTreeMap<String, Option<u64>>> = RefCell::default();
}

/// Starts a run without versions of an earlier one.
pub(crate) fn forget() {
    READ.with(|read| read.borrow_mut().clear());
}

/// The value of `key`, remembering its version for a later `set` or `remove`.
pub fn get<T: DeserializeOwned>(key: &str) -> Result<Option<T>> {
    let entry = host::with(|host| host.state_get(key))?;
    READ.with(|read| {
        read.borrow_mut()
            .insert(key.into(), entry.as_ref().map(|entry| entry.version))
    });
    entry
        .map(|entry| Ok(serde_json::from_slice(&entry.value)?))
        .transpose()
}

/// Writes `value` at the version this run read; a key it did not read must not exist yet, else
/// the write is a `Conflict`.
pub fn set<T: Serialize + ?Sized>(key: &str, value: &T) -> Result<()> {
    let bytes = serde_json::to_vec(value)?;
    let expected = READ.with(|read| read.borrow().get(key).copied().flatten());
    let version = host::with(|host| host.state_put(key, &bytes, expected))?;
    READ.with(|read| read.borrow_mut().insert(key.into(), Some(version)));
    Ok(())
}

/// Reads `key` (or takes `default`), applies `change` and writes the result, again after a
/// conflict up to five times; answers the value written.
pub fn update<T, F>(key: &str, default: T, mut change: F) -> Result<T>
where
    T: Serialize + DeserializeOwned + Clone,
    F: FnMut(T) -> T,
{
    let mut tried = 0;
    loop {
        let value = change(get(key)?.unwrap_or_else(|| default.clone()));
        match set(key, &value) {
            Err(Error::Conflict(_)) if tried + 1 < TRIES => tried += 1,
            written => return written.map(|()| value),
        }
    }
}

/// Removes `key` at the version this run read, reading it first when it did not; an absent key
/// is already removed.
pub fn remove(key: &str) -> Result<()> {
    let known = READ.with(|read| read.borrow().get(key).copied());
    let version = match known {
        Some(version) => version,
        None => {
            get::<serde_json::Value>(key)?;
            READ.with(|read| read.borrow().get(key).copied().flatten())
        }
    };
    let Some(version) = version else {
        return Ok(());
    };
    host::with(|host| host.state_remove(key, version))?;
    READ.with(|read| read.borrow_mut().insert(key.into(), None));
    Ok(())
}

#[cfg(test)]
#[path = "state_tests.rs"]
mod tests;
