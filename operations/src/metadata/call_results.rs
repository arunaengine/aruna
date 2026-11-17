//! Unpacks the call results metadata operations expect, refusing any other result kind.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoError, IoResult};
use aruna_core::events::Event;
use aruna_core::types::{Key, Value};

/// The values of a read call, one per requested key.
pub(crate) fn read_values(result: IoResult) -> Result<Vec<Option<Value>>, IoError> {
    match result {
        IoResult::Values(values) => Ok(values),
        other => Err(wrong_result("IoResult::Values", &other)),
    }
}

/// The value of a read call for exactly one key.
pub(crate) fn read_value(result: IoResult) -> Result<Option<Value>, IoError> {
    let mut values = read_values(result)?;
    match (values.pop(), values.is_empty()) {
        (Some(value), true) => Ok(value),
        _ => Err(IoError::Unexpected {
            expected: "one value",
            got: "another count of values",
        }),
    }
}

/// The rows of a page call and the key to continue after.
pub(crate) fn page_rows(result: IoResult) -> Result<(Vec<(Key, Value)>, Option<Key>), IoError> {
    match result {
        IoResult::Page { rows, next } => Ok((rows, next)),
        other => Err(wrong_result("IoResult::Page", &other)),
    }
}

/// The answer of a write, delete or sync call.
pub(crate) fn call_done(result: IoResult) -> Result<(), IoError> {
    match result {
        IoResult::Done => Ok(()),
        other => Err(wrong_result("IoResult::Done", &other)),
    }
}

/// The event that answered an effect call.
pub(crate) fn effect_event(result: IoResult) -> Result<Event, IoError> {
    match result {
        IoResult::Event(event) => Ok(event),
        other => Err(wrong_result("IoResult::Event", &other)),
    }
}

fn wrong_result(expected: &'static str, got: &IoResult) -> IoError {
    let got = match got {
        IoResult::Values(_) => "IoResult::Values",
        IoResult::Page { .. } => "IoResult::Page",
        IoResult::Done => "IoResult::Done",
        IoResult::Event(event) => event.label(),
        IoResult::Group(_) => "IoResult::Group",
        IoResult::Woke => "IoResult::Woke",
    };
    IoError::Unexpected { expected, got }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unpacks_expected_results() {
        let value: Value = vec![1].into();
        assert_eq!(
            read_value(IoResult::Values(vec![Some(value.clone())])),
            Ok(Some(value))
        );
        assert_eq!(call_done(IoResult::Done), Ok(()));
        let page = IoResult::Page {
            rows: Vec::new(),
            next: None,
        };
        assert_eq!(page_rows(page), Ok((Vec::new(), None)));
    }

    #[test]
    fn refuses_other_results() {
        assert!(matches!(
            read_values(IoResult::Done),
            Err(IoError::Unexpected {
                got: "IoResult::Done",
                ..
            })
        ));
        assert!(read_value(IoResult::Values(Vec::new())).is_err());
        assert!(read_value(IoResult::Values(vec![None, None])).is_err());
        assert!(page_rows(IoResult::Woke).is_err());
        assert!(effect_event(IoResult::Done).is_err());
    }
}
