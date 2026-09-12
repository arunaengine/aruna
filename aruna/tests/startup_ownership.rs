use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

#[path = "../../api/tests/support/syntax.rs"]
mod syntax;

const MAIN_SOURCE: &str = include_str!("../src/main.rs");

#[test]
fn startup_ownership() {
    let main = syntax::production(MAIN_SOURCE);

    assert_eq!(count(&main, ".recover_stale_jobs("), 1);
    assert_eq!(count(&main, "restore_drain_timer("), 1);

    let recovery = position(&main, ".recover_stale_jobs(");
    let runtime = position(&main, "jobs_runtime.start();");
    let queues = position(&main, "task_queues.start(&shutdown).await;");
    let timer = position(&main, "restore_drain_timer(");
    assert!(recovery < runtime && runtime < queues && queues < timer);

    let tasks = task_sources();
    for required in ["mod.rs", "outbox.rs", "restore.rs"] {
        assert!(
            tasks.contains_key(required),
            "task source {required} is missing"
        );
    }

    let task_recovery = tasks
        .values()
        .map(|source| count(source, ".recover_stale_jobs("))
        .sum::<usize>();
    let task_timer = tasks
        .values()
        .map(|source| count(source, "restore_drain_timer("))
        .sum::<usize>();
    assert_eq!(task_recovery, 0);
    assert_eq!(task_timer, 1);

    let restore = &tasks["restore.rs"];
    let queues_block = item_block(restore, "impl TaskQueues {");
    assert_eq!(count(queues_block, ".recover_stale_jobs("), 0);
    assert_eq!(count(queues_block, "restore_drain_timer("), 0);

    let rearm = item_block(restore, "async fn durable_rearm_loop(");
    assert_eq!(count(rearm, "restore_drain_timer("), 1);
}

/// Reads every production source beside the task module so a moved operation
/// cannot hide in a new child file.
fn task_sources() -> BTreeMap<String, String> {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../operations/src/tasks/incoming");
    let mut sources = BTreeMap::new();

    for entry in fs::read_dir(&dir).unwrap_or_else(|error| panic!("read {dir:?}: {error}")) {
        let path = entry
            .unwrap_or_else(|error| panic!("read entry: {error}"))
            .path();
        if path.extension().and_then(|extension| extension.to_str()) != Some("rs") {
            continue;
        }
        let name = path
            .file_name()
            .expect("task source has a file name")
            .to_string_lossy()
            .into_owned();
        let source =
            fs::read_to_string(&path).unwrap_or_else(|error| panic!("read {path:?}: {error}"));
        sources.insert(name, syntax::production(&source));
    }

    assert!(!sources.is_empty(), "task source directory is empty");
    sources
}

fn item_block<'a>(source: &'a str, needle: &str) -> &'a str {
    let start = position(source, needle);
    let open = source[start..]
        .find('{')
        .map(|at| start + at)
        .unwrap_or_else(|| panic!("{needle} must have a body"));
    let close = syntax::delimiter(source, open, b'{', b'}');
    &source[start..=close]
}

fn position(source: &str, needle: &str) -> usize {
    source
        .find(needle)
        .unwrap_or_else(|| panic!("{needle} must exist"))
}

fn count(source: &str, needle: &str) -> usize {
    source.match_indices(needle).count()
}
