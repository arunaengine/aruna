//! Builds the common storage calls of an operation and reads their results: rows, codecs, pages.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::calls::{IoCall, IoError, IoResult, Next};
use crate::operation::{Operation, TransactionMode};
use crate::types::{Key, KeySpace, Value};

/// The call that reads one row.
pub fn read_row(space: &str, key: impl Into<Key>) -> IoCall {
    IoCall::Read(vec![(space.to_string(), key.into())])
}

/// One call outside any transaction, run as a child of a host in a transaction.
#[derive(Debug, PartialEq)]
struct OutsideCall {
    call: Option<IoCall>,
}

impl Operation for OutsideCall {
    type Output = IoResult;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<IoResult>, IoError> {
        self.call.take().map(Next::Call).ok_or(IoError::Unfinished)
    }

    fn next(&mut self, result: IoResult) -> Result<Next<IoResult>, IoError> {
        Ok(Next::Done(result))
    }
}

/// `call` as a child of its own, so its failure reaches the caller as an answer.
pub fn outside_call(call: IoCall) -> IoCall {
    IoCall::child(OutsideCall { call: Some(call) })
}

/// The result of an [`outside_call`], or a result that answered the call directly.
pub fn outside_answer(result: IoResult) -> Result<IoResult, IoError> {
    match result {
        IoResult::Child(_) => result.child::<IoResult, IoError>()?,
        other => Ok(other),
    }
}

/// A call of an embedded update: one for the host's transaction, or a read that runs outside
/// it so that rows other writers change meanwhile never conflict the host's commit.
#[derive(Debug, PartialEq)]
pub enum StagedCall {
    InTransaction(IoCall),
    Outside(IoCall),
}

impl StagedCall {
    /// The call a host operation makes; an outside read runs as its own child run, which never
    /// joins the host's transaction.
    pub fn into_call(self) -> IoCall {
        match self {
            Self::InTransaction(call) => call,
            Self::Outside(call) => outside_call(call),
        }
    }
}

/// The `N` values of a read of `N` rows, in the order they were asked for.
pub fn values<const N: usize>(result: IoResult) -> Result<[Option<Value>; N], IoError> {
    <[_; N]>::try_from(result.values()?).map_err(|_| IoError::Unexpected {
        expected: "one value per row read",
        got: "a read of another length",
    })
}

/// The decoded value of a one-row read; a missing row is `None`.
pub fn decoded<T: DeserializeOwned>(result: IoResult) -> Result<Option<T>, IoError> {
    let [value] = values::<1>(result)?;
    value.map(|value| decode(&value)).transpose()
}

pub fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, IoError> {
    postcard::from_bytes(bytes).map_err(|error| IoError::Codec(error.to_string()))
}

pub fn encode<T: Serialize>(value: &T) -> Result<Value, IoError> {
    postcard::to_allocvec(value)
        .map(Value::from)
        .map_err(|error| IoError::Codec(error.to_string()))
}

/// One row to write with its value encoded, for an `IoCall::Write`.
pub fn encoded_row<T: Serialize>(
    space: &str,
    key: impl Into<Key>,
    value: &T,
) -> Result<(KeySpace, Key, Value), IoError> {
    Ok((space.to_string(), key.into(), encode(value)?))
}

/// Reads every row below a prefix one page at a time, so an operation handles each page before
/// it asks for the next one.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PrefixPages {
    space: KeySpace,
    prefix: Key,
    after: Option<Key>,
    limit: usize,
    done: bool,
}

impl PrefixPages {
    /// Pages of at most `limit` rows below `prefix`.
    pub fn new(space: &str, prefix: impl Into<Key>, limit: usize) -> Self {
        Self {
            space: space.to_string(),
            prefix: prefix.into(),
            after: None,
            limit,
            done: false,
        }
    }

    /// The call for the next page.
    pub fn call(&self) -> IoCall {
        IoCall::Page {
            space: self.space.clone(),
            prefix: self.prefix.clone(),
            after: self.after.clone(),
            limit: self.limit,
        }
    }

    /// The rows of the page that answered [`Self::call`].
    pub fn take(&mut self, result: IoResult) -> Result<Vec<(Key, Value)>, IoError> {
        let (rows, next) = result.page()?;
        self.done = next.is_none();
        self.after = next;
        Ok(rows)
    }

    /// Whether the last page was taken.
    pub fn done(&self) -> bool {
        self.done
    }
}

/// Deletes a set's marker row only while it still holds `seen`: a record installed since the
/// pass read it wrote a newer revision, and its work is looked at again.
#[derive(Debug, PartialEq)]
pub struct SettleMarkerOperation {
    key_space: String,
    key: Vec<u8>,
    seen: Vec<u8>,
    deleting: bool,
}

impl SettleMarkerOperation {
    pub fn new(key_space: &str, key: Vec<u8>, seen: Vec<u8>) -> Self {
        Self {
            key_space: key_space.to_string(),
            key,
            seen,
            deleting: false,
        }
    }

    fn row(&self) -> (String, Key) {
        (self.key_space.clone(), self.key.clone().into())
    }
}

impl Operation for SettleMarkerOperation {
    type Output = ();
    type Error = IoError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> Result<Next<()>, IoError> {
        Ok(Next::Call(IoCall::Read(vec![self.row()])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, IoError> {
        if self.deleting {
            result.done()?;
            return Ok(Next::Done(()));
        }
        if result.value()?.as_deref() != Some(self.seen.as_slice()) {
            return Ok(Next::Done(()));
        }
        self.deleting = true;
        Ok(Next::Call(IoCall::Delete(vec![self.row()])))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(text: &str) -> Key {
        Key::from(text.as_bytes())
    }

    #[test]
    fn decodes_written_row() {
        let (space, row_key, value) = encoded_row("rows", key("a"), &(7u32, "x")).unwrap();
        assert_eq!((space.as_str(), row_key), ("rows", key("a")));
        let read = IoResult::Values(vec![Some(value)]);
        assert_eq!(
            decoded::<(u32, String)>(read),
            Ok(Some((7, "x".to_string())))
        );
        assert_eq!(decoded::<u32>(IoResult::Values(vec![None])), Ok(None));
        let wrong = IoResult::Values(vec![None, None]);
        assert!(matches!(
            decoded::<u32>(wrong),
            Err(IoError::Unexpected { .. })
        ));
        assert_eq!(
            IoResult::Woke.done(),
            Err(IoError::Unexpected {
                expected: "IoResult::Done",
                got: "IoResult::Woke",
            })
        );
    }

    #[test]
    fn pages_until_done() {
        let mut pages = PrefixPages::new("rows", key("p/"), 2);
        let first = vec![(key("p/1"), key("1")), (key("p/2"), key("2"))];
        let next = Some(key("p/2"));
        let page = IoResult::Page {
            rows: first.clone(),
            next,
        };
        assert_eq!(pages.take(page), Ok(first));
        assert!(!pages.done());
        assert_eq!(
            pages.call(),
            IoCall::Page {
                space: "rows".to_string(),
                prefix: key("p/"),
                after: Some(key("p/2")),
                limit: 2,
            }
        );
        let last = IoResult::Page {
            rows: Vec::new(),
            next: None,
        };
        assert_eq!(pages.take(last), Ok(Vec::new()));
        assert!(pages.done());
    }
}
