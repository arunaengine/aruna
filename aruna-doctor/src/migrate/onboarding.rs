//! Writes the expiry index row of every onboarding secret stored before the index, so the
//! expiry-ordered list and the expiry cleanup see it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{Rewrites, row_note};
use crate::explorer::ExplorerError;
use aruna_core::keyspaces::ONBOARDING_KEYSPACE;
use aruna_core::onboarding::OnboardingSecretRecord;
use aruna_operations::onboarding::create_secret::{SECRET_RECORD_PREFIX, secret_expiry_key};
use fjall::{OptimisticTxDatabase, OptimisticTxKeyspace, Readable};

/// The index rows the stored secrets lack, with how many secrets were read. A secret that does
/// not decode is added to `unknown` and gets no row.
pub(super) fn expiry_rows(
    db: &OptimisticTxDatabase,
    keyspace: &OptimisticTxKeyspace,
    unknown: &mut Vec<String>,
) -> Result<Rewrites, ExplorerError> {
    let read = db.read_tx();
    let (mut scanned, mut rows) = (0, Vec::new());
    for entry in read.prefix(keyspace, SECRET_RECORD_PREFIX) {
        let (key, value) = entry.into_inner()?;
        scanned += 1;
        let record = match postcard::from_bytes::<OnboardingSecretRecord>(&value) {
            Ok(record) => record,
            Err(error) => {
                unknown.push(row_note(ONBOARDING_KEYSPACE, &key, error));
                continue;
            }
        };
        let index = secret_expiry_key(record.expires_at, record.enrollment_id);
        if read.get(keyspace, &index)?.is_none() {
            rows.push((index.to_vec(), Vec::new()));
        }
    }
    Ok(Rewrites { scanned, rows })
}

#[cfg(test)]
mod tests {
    use crate::migrate::migrate_output;
    use crate::migrate::tests::{read, write};
    use aruna_core::keyspaces::ONBOARDING_KEYSPACE;
    use aruna_core::onboarding::{OnboardingMode, OnboardingPurpose, OnboardingSecretRecord};
    use aruna_operations::onboarding::create_secret::{secret_expiry_key, secret_record_key};
    use ulid::Ulid;

    // A secret stored before the expiry index gains its row, which sorts it by expiry; the
    // state row beside it gets none, and a second run writes nothing.
    #[test]
    fn indexes_old_secrets() {
        let record = OnboardingSecretRecord {
            enrollment_id: Ulid::from_bytes([5; 16]),
            secret_hash: "hash".to_string(),
            mode: OnboardingMode::Server,
            purpose: OnboardingPurpose::NodeEnrollment,
            expires_at: 77,
            claimed_node_id: None,
        };
        let key = secret_record_key(record.enrollment_id);
        let state = b"secret-state:any".as_slice();
        let temp = tempfile::tempdir().expect("temporary directory");
        let path = temp.path().join("db");
        let database = path.to_str().expect("path");
        let bytes = postcard::to_allocvec(&record).expect("record encodes");
        let rows = vec![(key.as_ref(), bytes), (state, vec![1])];
        write(&path, ONBOARDING_KEYSPACE, rows);

        let output = migrate_output(database).expect("migration runs");
        assert_eq!(output.onboarding_secrets_scanned, 1);
        assert_eq!(output.expiry_rows_written, 1);
        let stored = read(&path, ONBOARDING_KEYSPACE);
        let index = secret_expiry_key(record.expires_at, record.enrollment_id);
        assert_eq!(stored.get(index.as_ref()), Some(&Vec::new()));
        assert_eq!(stored.len(), 3);

        let again = migrate_output(database).expect("migration repeats");
        assert_eq!(again.expiry_rows_written, 0);
        assert_eq!(read(&path, ONBOARDING_KEYSPACE), stored);
    }
}
