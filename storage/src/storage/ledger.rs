//! Debug builds append each outbox row write to the file `ARUNA_TEST_OUTBOX_LEDGER` names, so a
//! test can see whether a run attempted an outbox enqueue.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::io::Write;
use std::sync::Mutex;

use aruna_core::document::DocumentOutboxRecord;
use aruna_core::effects::StorageEffect;
use aruna_core::keyspaces::SYNC_OUTBOX_KEYSPACE;

/// Keeps the lines of concurrent writes whole.
static LOCK: Mutex<()> = Mutex::new(());

/// Records the outbox rows `effect` writes, when the ledger is switched on.
pub(super) fn record_outbox(effect: &StorageEffect) {
    let Some(path) = std::env::var_os("ARUNA_TEST_OUTBOX_LEDGER") else {
        return;
    };
    let rows: Vec<_> = match effect {
        StorageEffect::Write {
            key_space,
            key,
            value,
            ..
        } => vec![(key_space, key, value)],
        StorageEffect::BatchWrite { writes, .. } => (writes.iter())
            .map(|(key_space, key, value)| (key_space, key, value))
            .collect(),
        _ => return,
    };
    let lines: String = (rows.into_iter())
        .filter(|(key_space, _, _)| key_space.as_str() == SYNC_OUTBOX_KEYSPACE)
        .map(|(_, key, value)| {
            let target = postcard::from_bytes::<DocumentOutboxRecord>(value)
                .map(|record| format!("{:?}", record.target))
                .unwrap_or_default();
            let key_hash = blake3::hash(key).to_hex();
            let value_hash = blake3::hash(value).to_hex();
            format!("target={target} key_hash={key_hash} value_hash={value_hash}\n")
        })
        .collect();
    if lines.is_empty() {
        return;
    }
    let Ok(_guard) = LOCK.lock() else {
        return;
    };
    let file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path);
    if let Ok(mut file) = file {
        let _ = file.write_all(lines.as_bytes());
    }
}
