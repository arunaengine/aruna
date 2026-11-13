//! Rewrites stored plugin events from before they named their actor (alpha.190): journal rows,
//! forward and waiting rows, and runs an event started. The actor stays absent for them.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::session_refs::{self, Layout};
use super::{Rewrites, decode_error};
use crate::explorer::ExplorerError;
use fjall::{OptimisticTxDatabase, OptimisticTxKeyspace, Readable};
use serde::Serialize;
use serde::de::DeserializeOwned;

/// Events before the actor, stored with their first eight fields.
const EVENT: Layout = &[("SourceEvent", 8)];

/// The rows of `keyspace` whose key starts with one of `prefixes`, or every row when there are
/// none, that hold an event without its actor, in the current shape of `T`.
pub(super) fn event_rows<T: Serialize + DeserializeOwned>(
    db: &OptimisticTxDatabase,
    keyspace: &OptimisticTxKeyspace,
    (name, prefixes): (&str, &[u8]),
) -> Result<Rewrites, ExplorerError> {
    let mut scanned = 0;
    let mut rows = Vec::new();
    for entry in db.read_tx().iter(keyspace) {
        let (key, value) = entry.into_inner()?;
        let kind = key.first().copied();
        if !prefixes.is_empty() && !kind.is_some_and(|kind| prefixes.contains(&kind)) {
            continue;
        }
        scanned += 1;
        if postcard::from_bytes::<T>(&value).is_ok() {
            continue;
        }
        let old = session_refs::decode::<T>(&value, EVENT);
        let old = old.map_err(|error| decode_error(name, &key, error))?;
        let bytes = postcard::to_allocvec(&old).map_err(|error| decode_error(name, &key, error))?;
        rows.push((key.to_vec(), bytes));
    }
    Ok(Rewrites { scanned, rows })
}

#[cfg(test)]
mod tests {
    use super::super::migrate_output;
    use super::super::tests::{read, strip, write};
    use aruna_core::keyspaces::{
        EVENT_JOURNAL_KEYSPACE, PLUGIN_OUTBOX_KEYSPACE, PLUGIN_RUNS_KEYSPACE,
        PLUGIN_WAITING_KEYSPACE,
    };
    use aruna_core::plugin_runs::{
        ForwardRow, PluginRun, RunState, RunTrigger, SourceEvent, SourceKind,
    };
    use aruna_operations::plugins::decide::WaitingRow;
    use ulid::Ulid;

    fn event() -> SourceEvent {
        SourceEvent {
            kind: SourceKind::DatasetCreated,
            group_id: Ulid::from_parts(1, 1),
            path: "/r/g/x/meta/d".into(),
            resource: Ulid::from_parts(2, 2).to_string(),
            revision: Ulid::from_parts(3, 3).to_string(),
            source: "dataset_created/e".into(),
            anchor_ms: 3,
            journaled_ms: 4,
            actor: None,
        }
    }

    fn run(event: SourceEvent) -> PluginRun {
        PluginRun {
            run_id: Ulid::from_parts(5, 5),
            install_id: Ulid::from_parts(6, 6),
            group_id: Ulid::from_parts(1, 1),
            plugin: "announcer".into(),
            version: "0.1.0".into(),
            revision: 1,
            digest: format!("sha256:{}", "ab".repeat(32)),
            handler: "echo".into(),
            settings: "{}".into(),
            trigger: RunTrigger::Event(event),
            state: RunState::Queued,
            attempts: 0,
            due_ms: 1,
            created_ms: 1,
            finished_ms: None,
            message: None,
            output: None,
            log: Vec::new(),
        }
    }

    /// The bytes of `value` as alpha.190 stored them: its event without the actor's byte.
    fn old<T: serde::Serialize>(value: &T) -> Vec<u8> {
        let current = postcard::to_allocvec(value).unwrap();
        let event = postcard::to_allocvec(&event()).unwrap();
        strip(&current, &event, 1)
    }

    #[test]
    fn rewrites_event_rows() {
        let forward = ForwardRow {
            install_id: Ulid::from_parts(6, 6),
            revision: 2,
            event: event(),
            queued_ms: 9,
        };
        let waiting = WaitingRow {
            row: forward.clone(),
            received_ms: 10,
        };
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("db");
        let database = path.to_str().unwrap();
        // A source-id row and a timer row are other kinds and stay as they are.
        let journal = vec![(&b"jrow"[..], old(&event())), (&b"iid"[..], vec![0, 1, 2])];
        write(&path, EVENT_JOURNAL_KEYSPACE, journal);
        write(&path, PLUGIN_OUTBOX_KEYSPACE, vec![(b"out", old(&forward))]);
        write(
            &path,
            PLUGIN_WAITING_KEYSPACE,
            vec![(b"wait", old(&waiting))],
        );
        let runs = vec![(&b"rrun"[..], old(&run(event()))), (&b"timer"[..], vec![1])];
        write(&path, PLUGIN_RUNS_KEYSPACE, runs);

        let output = migrate_output(database).unwrap();
        assert_eq!(output.plugin_events_rewritten, 4);
        let journal = read(&path, EVENT_JOURNAL_KEYSPACE);
        let stored: SourceEvent = postcard::from_bytes(&journal[b"jrow".as_slice()]).unwrap();
        assert_eq!(stored, event());
        assert_eq!(journal[b"iid".as_slice()], vec![0, 1, 2]);
        let outbox = read(&path, PLUGIN_OUTBOX_KEYSPACE);
        let stored: ForwardRow = postcard::from_bytes(&outbox[b"out".as_slice()]).unwrap();
        assert_eq!(stored, forward);
        let waits = read(&path, PLUGIN_WAITING_KEYSPACE);
        let stored: WaitingRow = postcard::from_bytes(&waits[b"wait".as_slice()]).unwrap();
        assert_eq!(stored, waiting);
        let runs = read(&path, PLUGIN_RUNS_KEYSPACE);
        let stored: PluginRun = postcard::from_bytes(&runs[b"rrun".as_slice()]).unwrap();
        assert_eq!(stored, run(event()));
        assert_eq!(runs[b"timer".as_slice()], vec![1]);
        assert_eq!(migrate_output(database).unwrap().plugin_events_rewritten, 0);
    }
}
