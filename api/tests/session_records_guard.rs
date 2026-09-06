//! Cell traffic must never become a job record: the family log is capped per
//! family, so a session that wrote one record per cell would lock itself out.

use std::fs;
use std::path::{Path, PathBuf};

/// Symbols that write or transition a job record.
const WRITERS: &[&str] = &[
    "put_job_entry",
    "insert_job",
    "complete_job",
    "complete_execution",
    "complete_cancelled",
    "fail_execution",
    "cancel_execution",
    "mark_indeterminate",
    "record_attempt",
    "publish_state",
    "publish_terminal",
    "renew_lease",
    "transition_to_",
    "storage_handle",
];

/// The session path: the routes and the node-local manager behind them.
fn sources(manifest_dir: &Path) -> Vec<PathBuf> {
    let mut paths = vec![manifest_dir.join("src/routes/job_session.rs")];
    let session = manifest_dir.join("../compute/src/session");
    if let Ok(entries) = fs::read_dir(&session) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().is_some_and(|extension| extension == "rs") {
                paths.push(path);
            }
        }
    }
    paths
}

#[test]
fn cells_write_no_records() {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut found = Vec::new();
    for path in sources(manifest_dir) {
        let source = fs::read_to_string(&path).unwrap_or_else(|error| {
            panic!("session source {} is readable: {error}", path.display())
        });
        for (index, line) in source.lines().enumerate() {
            for writer in WRITERS {
                if line.contains(writer) {
                    found.push(format!("{}:{}: {writer}", path.display(), index + 1));
                }
            }
        }
    }
    assert!(
        found.is_empty(),
        "the session path must write no job records, found:\n{}",
        found.join("\n")
    );
}
