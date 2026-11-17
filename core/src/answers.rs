//! Reads the answer an operation expects from an `IoResult`, or names the answer it got.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::calls::{IoError, IoResult};
use crate::events::Event;
use crate::types::{Key, Value};

impl IoResult {
    /// The values of an `IoCall::Read`, one per requested row.
    pub fn values(self) -> Result<Vec<Option<Value>>, IoError> {
        match self {
            Self::Values(values) => Ok(values),
            other => Err(other.unexpected("IoResult::Values")),
        }
    }

    /// The value of an `IoCall::Read` of exactly one row.
    pub fn value(self) -> Result<Option<Value>, IoError> {
        match self {
            Self::Values(values) if values.len() == 1 => Ok(values.into_iter().next().flatten()),
            other => Err(other.unexpected("IoResult::Values with one value")),
        }
    }

    /// The rows of an `IoCall::Page` and the key to continue after.
    pub fn page(self) -> Result<(Vec<(Key, Value)>, Option<Key>), IoError> {
        match self {
            Self::Page { rows, next } => Ok((rows, next)),
            other => Err(other.unexpected("IoResult::Page")),
        }
    }

    /// The end of an `IoCall::Write`, `IoCall::Delete` or `IoCall::Sync`.
    pub fn done(self) -> Result<(), IoError> {
        match self {
            Self::Done => Ok(()),
            other => Err(other.unexpected("IoResult::Done")),
        }
    }

    /// The event that answered an `IoCall::Effect` or an `IoCall::Child`.
    pub fn event(self) -> Result<Event, IoError> {
        match self {
            Self::Event(event) => Ok(event),
            other => Err(other.unexpected("IoResult::Event")),
        }
    }

    /// The name of this answer's kind, for an unexpected answer.
    pub fn label(&self) -> &'static str {
        match self {
            Self::Values(_) => "IoResult::Values",
            Self::Page { .. } => "IoResult::Page",
            Self::Done => "IoResult::Done",
            Self::Event(event) => event.label(),
            Self::Group(_) => "IoResult::Group",
            Self::Woke => "IoResult::Woke",
        }
    }

    fn unexpected(&self, expected: &'static str) -> IoError {
        IoError::Unexpected {
            expected,
            got: self.label(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_expected_answers() {
        let value: Value = b"v".to_vec().into();
        let one = IoResult::Values(vec![Some(value.clone())]);
        assert_eq!(one.value(), Ok(Some(value.clone())));
        assert_eq!(IoResult::Values(vec![None]).value(), Ok(None));
        assert_eq!(IoResult::Done.done(), Ok(()));
        let page = IoResult::Page {
            rows: Vec::new(),
            next: None,
        };
        assert_eq!(page.page(), Ok((Vec::new(), None)));
        assert_eq!(IoResult::Event(Event::Delayed).event(), Ok(Event::Delayed));
    }

    #[test]
    fn names_other_answers() {
        let two = IoResult::Values(vec![None, None]);
        assert_eq!(
            two.value(),
            Err(IoError::Unexpected {
                expected: "IoResult::Values with one value",
                got: "IoResult::Values",
            })
        );
        assert_eq!(
            IoResult::Woke.done(),
            Err(IoError::Unexpected {
                expected: "IoResult::Done",
                got: "IoResult::Woke",
            })
        );
        let event = IoResult::Event(Event::Delayed);
        assert!(matches!(
            event.values(),
            Err(IoError::Unexpected {
                expected: "IoResult::Values",
                ..
            })
        ));
    }
}
