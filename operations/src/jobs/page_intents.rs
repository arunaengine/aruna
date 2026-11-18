//! Reads the write intents of a listing page of a backend import in one key range.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::IMPORT_INTENT_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::storage::backend_import::{
    ExternalObject, WriteIntent, object_key, object_prefix,
};
use aruna_core::types::Key;
use ulid::Ulid;

/// Intent rows one read returns.
const INTENT_PAGE: usize = 500;

/// The write intents of listed paths, by path.
pub(crate) type PathIntents = BTreeMap<String, Vec<(Key, WriteIntent)>>;

/// Why the write intents of a page could not be read.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub(crate) struct IntentReadError(pub(crate) String);

impl From<IoError> for IntentReadError {
    fn from(error: IoError) -> Self {
        Self(error.to_string())
    }
}

/// The write intents of every path from the first to the last object of a listing page, read
/// as one range, so a page of known files costs one read however many objects it has.
#[derive(Debug, PartialEq)]
pub(crate) struct PageIntentsOperation {
    import_id: Ulid,
    /// The page's first and last path; `None` for an empty page.
    bounds: Option<(String, String)>,
    intents: PathIntents,
}

impl PageIntentsOperation {
    pub(crate) fn new(import_id: Ulid, objects: &[ExternalObject]) -> Self {
        let paths = objects.iter().map(|object| object.key.as_str());
        let bounds = paths.clone().min().zip(paths.max());
        Self {
            import_id,
            bounds: bounds.map(|(first, last)| (first.to_string(), last.to_string())),
            intents: PathIntents::new(),
        }
    }

    fn page(&self, start: IterStart) -> Next<PathIntents> {
        let iter = StorageEffect::Iter {
            key_space: IMPORT_INTENT_KEYSPACE.to_string(),
            prefix: Some(object_prefix(self.import_id)),
            start: Some(start),
            limit: INTENT_PAGE,
            txn_id: None,
        };
        Next::Call(IoCall::Effect(Box::new(Effect::Storage(iter))))
    }
}

impl Operation for PageIntentsOperation {
    type Output = PathIntents;
    type Error = IntentReadError;

    fn first(&mut self) -> Result<Next<PathIntents>, IntentReadError> {
        let Some((first, _)) = self.bounds.as_ref() else {
            return Ok(Next::Done(PathIntents::new()));
        };
        let start = IterStart::At(object_key(self.import_id, first));
        Ok(self.page(start))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<PathIntents>, IntentReadError> {
        let event = result.event()?;
        let Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) = event
        else {
            return Err(IntentReadError(format!(
                "unexpected answer {}",
                event.label()
            )));
        };
        let last = (self.bounds.as_ref()).map_or("", |(_, last)| last.as_str());
        for (key, value) in values {
            let Some(path) = intent_path(&key) else {
                return Err(IntentReadError(
                    "a malformed write intent key".to_string(),
                ));
            };
            if path > last {
                return Ok(Next::Done(std::mem::take(&mut self.intents)));
            }
            let path = path.to_string();
            let intent = WriteIntent::from_bytes(&value)
                .map_err(|error| IntentReadError(error.to_string()))?;
            self.intents.entry(path).or_default().push((key, intent));
        }
        match next_start_after {
            Some(next) => Ok(self.page(IterStart::After(next))),
            None => Ok(Next::Done(std::mem::take(&mut self.intents))),
        }
    }
}

/// The path of an intent key: the import id, the path, a zero byte and the version id.
pub(crate) fn intent_path(key: &[u8]) -> Option<&str> {
    let end = key.len().checked_sub(17)?;
    let path = key.get(16..end)?;
    (key[end] == 0).then_some(())?;
    std::str::from_utf8(path).ok()
}

#[cfg(test)]
#[path = "page_intents_tests.rs"]
mod tests;
