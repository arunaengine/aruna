use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

const SCAN_DIRS: &[&str] = &["src/routes", "src/s3"];
const PATTERNS: &[&str] = &[
    "send_effect",
    "send_storage_effect",
    "StorageEffect::",
    "MetadataEffect::",
    "TaskEffect::",
    "NetEffect::",
];

const ALLOWLIST: &[(&str, usize, &str, &str)] = &[
    (
        "src/routes/users.rs",
        259,
        "send_effect|StorageEffect::",
        ".send_effect(Effect::Storage(StorageEffect::Read {",
    ),
    (
        "src/routes/users.rs",
        290,
        "send_effect|StorageEffect::",
        ".send_effect(Effect::Storage(StorageEffect::Read {",
    ),
    (
        "src/routes/users.rs",
        327,
        "send_effect|StorageEffect::",
        ".send_effect(Effect::Storage(StorageEffect::Read {",
    ),
    (
        "src/routes/users.rs",
        355,
        "send_effect|StorageEffect::",
        ".send_effect(Effect::Storage(StorageEffect::Read {",
    ),
    (
        "src/routes/users.rs",
        1107,
        "send_effect|StorageEffect::",
        ".send_effect(Effect::Storage(StorageEffect::Read {",
    ),
    (
        "src/routes/users.rs",
        1197,
        "send_effect|StorageEffect::",
        ".send_effect(Effect::Storage(StorageEffect::Write {",
    ),
    (
        "src/routes/users.rs",
        1467,
        "send_effect|StorageEffect::",
        ".send_effect(Effect::Storage(StorageEffect::Write {",
    ),
    (
        "src/routes/users.rs",
        1619,
        "send_effect|StorageEffect::",
        ".send_effect(Effect::Storage(StorageEffect::Write {",
    ),
    (
        "src/s3/s3_service.rs",
        497,
        "send_storage_effect|StorageEffect::",
        ".send_storage_effect(StorageEffect::StartTransaction { read: false })",
    ),
    (
        "src/s3/s3_service.rs",
        507,
        "send_storage_effect|StorageEffect::",
        ".send_storage_effect(StorageEffect::Read {",
    ),
    (
        "src/s3/s3_service.rs",
        577,
        "send_storage_effect|StorageEffect::",
        ".send_storage_effect(StorageEffect::Write {",
    ),
    (
        "src/s3/s3_service.rs",
        599,
        "send_storage_effect|StorageEffect::",
        ".send_storage_effect(StorageEffect::CommitTransaction { txn_id })",
    ),
    (
        "src/s3/s3_service.rs",
        617,
        "send_storage_effect|StorageEffect::",
        ".send_storage_effect(StorageEffect::AbortTransaction { txn_id })",
    ),
    (
        "src/s3/s3_service.rs",
        1469,
        "send_storage_effect|StorageEffect::",
        ".send_storage_effect(StorageEffect::Write {",
    ),
    (
        "src/s3/s3_service.rs",
        1490,
        "send_storage_effect|StorageEffect::",
        ".send_storage_effect(StorageEffect::Read {",
    ),
    (
        "src/s3/s3_service.rs",
        1571,
        "send_storage_effect|StorageEffect::",
        ".send_storage_effect(StorageEffect::Write {",
    ),
    (
        "src/s3/s3_service.rs",
        1596,
        "send_storage_effect|StorageEffect::",
        ".send_storage_effect(StorageEffect::Write {",
    ),
    (
        "src/s3/s3_service.rs",
        1622,
        "send_storage_effect|StorageEffect::",
        ".send_storage_effect(StorageEffect::Write {",
    ),
    (
        "src/s3/s3_service.rs",
        1650,
        "send_storage_effect|StorageEffect::",
        ".send_storage_effect(StorageEffect::Write {",
    ),
    (
        "src/s3/s3_service.rs",
        1662,
        "send_storage_effect|StorageEffect::",
        ".send_storage_effect(StorageEffect::Write {",
    ),
    (
        "src/routes/staging.rs",
        596,
        "send_storage_effect|StorageEffect::",
        ".send_storage_effect(StorageEffect::Write {",
    ),
    (
        "src/routes/onboarding.rs",
        526,
        "send_effect|StorageEffect::",
        ".send_effect(Effect::Storage(StorageEffect::StartTransaction {",
    ),
    (
        "src/routes/onboarding.rs",
        544,
        "send_effect|StorageEffect::",
        ".send_effect(Effect::Storage(StorageEffect::Read {",
    ),
    (
        "src/routes/onboarding.rs",
        611,
        "send_effect|StorageEffect::",
        ".send_effect(Effect::Storage(StorageEffect::Write {",
    ),
    (
        "src/routes/onboarding.rs",
        634,
        "send_effect|StorageEffect::",
        ".send_effect(Effect::Storage(StorageEffect::CommitTransaction { txn_id }))",
    ),
    (
        "src/routes/onboarding.rs",
        649,
        "send_effect|StorageEffect::",
        ".send_effect(Effect::Storage(StorageEffect::AbortTransaction { txn_id }))",
    ),
    (
        "src/routes/onboarding.rs",
        949,
        "send_effect|StorageEffect::",
        ".send_effect(Effect::Storage(StorageEffect::Read {",
    ),
    (
        "src/routes/info.rs",
        352,
        "send_effect|StorageEffect::",
        ".send_effect(Effect::Storage(StorageEffect::Read {",
    ),
    (
        "src/routes/info.rs",
        780,
        "send_storage_effect|StorageEffect::",
        ".send_storage_effect(StorageEffect::Read {",
    ),
    (
        "src/routes/connectors.rs",
        405,
        "send_effect",
        ".send_effect(read_connector_secret_effect(connector_id, None))",
    ),
    (
        "src/routes/connectors.rs",
        718,
        "send_storage_effect|StorageEffect::",
        ".send_storage_effect(StorageEffect::Write {",
    ),
    (
        "src/routes/metadata.rs",
        1639,
        "send_effect",
        ".send_effect(read_registry_by_document_effect(document_id, None))",
    ),
    (
        "src/routes/metadata.rs",
        1667,
        "send_effect",
        ".send_effect(read_materialization_status_effect(record.document_id, None))",
    ),
    (
        "src/routes/metadata.rs",
        1704,
        "send_effect|MetadataEffect::",
        ".send_effect(Effect::Metadata(MetadataEffect::ExportRoCrate {",
    ),
    (
        "src/routes/metadata.rs",
        1727,
        "send_effect|MetadataEffect::",
        ".send_effect(Effect::Metadata(MetadataEffect::ExportRoCrateSummary {",
    ),
    (
        "src/routes/metadata.rs",
        1755,
        "send_effect|MetadataEffect::",
        ".send_effect(Effect::Metadata(MetadataEffect::ExportRoCratePage {",
    ),
    (
        "src/routes/metadata.rs",
        2905,
        "send_effect",
        "match ctx.storage_handle.send_effect(effect).await {",
    ),
    (
        "src/routes/metadata.rs",
        2912,
        "send_storage_effect|StorageEffect::",
        ".send_storage_effect(StorageEffect::BatchDelete {",
    ),
    (
        "src/routes/metadata.rs",
        3598,
        "send_effect|StorageEffect::",
        ".send_effect(Effect::Storage(StorageEffect::Write {",
    ),
    (
        "src/routes/metadata.rs",
        3617,
        "send_storage_effect|StorageEffect::",
        ".send_storage_effect(StorageEffect::Read {",
    ),
];

#[derive(Debug, Eq, Ord, PartialEq, PartialOrd)]
struct GuardMatch {
    path: String,
    line: usize,
    patterns: String,
    text: String,
}

#[test]
fn api_routes_and_s3_direct_side_effects_stay_allowlisted() {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let actual = scan_sources(manifest_dir);
    let allowed = allowlist();

    let unexpected = actual.difference(&allowed).collect::<Vec<_>>();
    let stale = allowed.difference(&actual).collect::<Vec<_>>();

    if !unexpected.is_empty() || !stale.is_empty() {
        panic!(
            "API direct side-effect orchestration allowlist drifted. \
             Refactor new matches out of api routes/S3, or update the temporary allowlist.\n\n\
             Unexpected matches:\n{}\nStale allowlist entries:\n{}",
            format_matches(&unexpected),
            format_matches(&stale),
        );
    }
}

fn allowlist() -> BTreeSet<GuardMatch> {
    ALLOWLIST
        .iter()
        .map(|(path, line, patterns, text)| GuardMatch {
            path: (*path).to_owned(),
            line: *line,
            patterns: (*patterns).to_owned(),
            text: (*text).to_owned(),
        })
        .collect()
}

fn scan_sources(manifest_dir: &Path) -> BTreeSet<GuardMatch> {
    let mut files = Vec::new();

    for dir in SCAN_DIRS {
        collect_rs_files(&manifest_dir.join(dir), &mut files);
    }

    files.sort();

    files
        .into_iter()
        .flat_map(|path| scan_file(manifest_dir, &path))
        .collect()
}

fn collect_rs_files(dir: &Path, files: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(dir).unwrap_or_else(|err| panic!("failed to read {dir:?}: {err}")) {
        let path = entry
            .unwrap_or_else(|err| panic!("failed to read entry in {dir:?}: {err}"))
            .path();

        if path.is_dir() {
            collect_rs_files(&path, files);
        } else if path.extension().and_then(|extension| extension.to_str()) == Some("rs") {
            files.push(path);
        }
    }
}

fn scan_file(manifest_dir: &Path, path: &Path) -> Vec<GuardMatch> {
    let contents = fs::read_to_string(path)
        .unwrap_or_else(|err| panic!("failed to read source file {path:?}: {err}"));
    let relative_path = path
        .strip_prefix(manifest_dir)
        .unwrap_or_else(|err| panic!("failed to make {path:?} relative: {err}"))
        .to_string_lossy()
        .replace('\\', "/");

    contents
        .lines()
        .enumerate()
        .filter_map(|(index, line)| {
            let patterns = PATTERNS
                .iter()
                .copied()
                .filter(|pattern| line.contains(pattern))
                .collect::<Vec<_>>();

            (!patterns.is_empty()).then(|| GuardMatch {
                path: relative_path.clone(),
                line: index + 1,
                patterns: patterns.join("|"),
                text: line.trim().to_owned(),
            })
        })
        .collect()
}

fn format_matches(matches: &[&GuardMatch]) -> String {
    if matches.is_empty() {
        return "    none\n".to_owned();
    }

    matches
        .iter()
        .map(|mat| {
            format!(
                "    ({:?}, {}, {:?}, {:?}),\n",
                mat.path, mat.line, mat.patterns, mat.text
            )
        })
        .collect()
}
