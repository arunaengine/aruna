//! Converts ISA metadata, records ARC snapshots and merges them into main.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::git::{command, exchange};
use aruna_core::git::{GitSnapshot, LfsObject, LinkedObject, MAX_GIT_BYTES, MergeOutcome, Refs};
use aruna_core::repo_layout::{ARUNA_FILE, CRATE_FILE, INVESTIGATION, Layout, metadata_layout};
use aruna_core::structs::storage::dataset_location::DatasetLocation;
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use bytes::Bytes;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::path::Path;
use tokio::process::Command;
use ulid::Ulid;

pub async fn convert(request: Value) -> std::io::Result<Value> {
    let mut process = Command::new("python3");
    process.args(["-c", include_str!("arc.py")]);
    let bytes = serde_json::to_vec(&request).map_err(std::io::Error::other)?;
    if bytes.len() > MAX_GIT_BYTES {
        return Err(std::io::Error::other("ARC conversion exceeds limit"));
    }
    let output = exchange(process, bytes.into(), false).await?;
    serde_json::from_slice(&output).map_err(std::io::Error::other)
}

pub async fn export(directory: &Path, revision: &str) -> std::io::Result<Value> {
    if revision.is_empty() || revision.starts_with('-') || revision.len() > 256 {
        return Err(std::io::Error::other("invalid Git revision"));
    }
    let oid = command(
        directory,
        &["rev-parse", "--verify", &format!("{revision}^{{commit}}")],
    )
    .await?;
    let oid = std::str::from_utf8(&oid)
        .map_err(std::io::Error::other)?
        .trim();
    if crate::rocrate::layout(directory, oid).await? == Layout::RoCrate {
        return crate::rocrate::export(directory, oid).await;
    }
    let tree = command(directory, &["ls-tree", "-rlz", oid]).await?;
    let mut files = BTreeMap::new();
    let mut total = 0usize;
    for row in tree.split(|byte| *byte == 0).filter(|row| !row.is_empty()) {
        let row = std::str::from_utf8(row).map_err(std::io::Error::other)?;
        let (header, path) = row
            .split_once('\t')
            .ok_or_else(|| std::io::Error::other("invalid Git tree"))?;
        let fields: Vec<_> = header.split_whitespace().collect();
        if fields.len() != 4
            || fields[1] != "blob"
            || !matches!(fields[0], "100644" | "100755")
            || files.len() >= 10000
        {
            return Err(std::io::Error::other("unsupported ARC tree"));
        }
        let data = command(directory, &["cat-file", "blob", fields[2]]).await?;
        total = total.saturating_add(data.len());
        if total > MAX_GIT_BYTES / 2 {
            return Err(std::io::Error::other(
                "ARC Git files exceed limit; use LFS for large data",
            ));
        }
        files.insert(path.to_string(), STANDARD.encode(data));
    }
    let mut result = convert(json!({"mode":"inspect", "files":files})).await?;
    result["commit"] = json!(oid);
    Ok(result)
}

fn failed(error: &str) -> std::io::Error {
    std::io::Error::other(error.to_string())
}

pub(crate) type Files = BTreeMap<String, (String, Vec<u8>)>;

/// Writes `base` (or an empty tree) with `files` replaced and `removed` paths dropped.
async fn write_tree(
    directory: &Path,
    base: Option<&str>,
    files: &Files,
    removed: &[String],
) -> std::io::Result<String> {
    let temporary = tempfile::tempdir_in(directory)?;
    let index = temporary.path().join("index");
    let git = |args: &[&str]| {
        let mut process = Command::new("git");
        process
            .current_dir(directory)
            .env("GIT_INDEX_FILE", &index)
            .args(args);
        process
    };
    if let Some(base) = base {
        exchange(git(&["read-tree", base]), Bytes::new(), false).await?;
    }
    for path in removed {
        exchange(
            git(&["update-index", "--force-remove", "--", path]),
            Bytes::new(),
            false,
        )
        .await?;
    }
    for (path, (mode, data)) in files {
        let oid = exchange(
            git(&["hash-object", "-w", "--stdin"]),
            data.clone().into(),
            false,
        )
        .await?;
        let oid = std::str::from_utf8(&oid)
            .map_err(std::io::Error::other)?
            .trim();
        let arguments = ["update-index", "--add", "--cacheinfo", mode, oid, path];
        exchange(git(&arguments), Bytes::new(), false).await?;
    }
    let tree = exchange(git(&["write-tree"]), Bytes::new(), false).await?;
    Ok(std::str::from_utf8(&tree)
        .map_err(std::io::Error::other)?
        .trim()
        .to_string())
}

async fn commit_tree(
    directory: &Path,
    tree: &str,
    parents: &[&str],
    message: String,
    occurred_at_ms: u64,
) -> std::io::Result<String> {
    let mut process = Command::new("git");
    let date = format!("@{} +0000", occurred_at_ms / 1000);
    process
        .current_dir(directory)
        .args(["commit-tree", "--no-gpg-sign", tree])
        .env("GIT_AUTHOR_NAME", "Aruna")
        .env("GIT_COMMITTER_NAME", "Aruna")
        .env("GIT_AUTHOR_EMAIL", "git@aruna.local")
        .env("GIT_COMMITTER_EMAIL", "git@aruna.local")
        .env("GIT_AUTHOR_DATE", &date)
        .env("GIT_COMMITTER_DATE", &date);
    for parent in parents {
        process.args(["-p", parent]);
    }
    let commit = exchange(process, message.into(), false).await?;
    Ok(std::str::from_utf8(&commit)
        .map_err(std::io::Error::other)?
        .trim()
        .to_string())
}

fn workbook(path: &str) -> bool {
    let name = path.rsplit('/').next().unwrap_or(path);
    name.starts_with("isa.") && name.ends_with(".xlsx")
}

fn entities(value: &Value) -> Vec<String> {
    let mut graph: Vec<_> = value["@graph"]
        .as_array()
        .into_iter()
        .flatten()
        .map(Value::to_string)
        .collect();
    graph.sort();
    graph
}

/// Brings graph changes onto a client-edited main without replacing equivalent client files.
/// Workbooks change only when their ISA meaning differs from the graph's generated ARC.
async fn reconcile(
    directory: &Path,
    main: &str,
    files: &Files,
    pointers: &std::collections::BTreeSet<String>,
) -> std::io::Result<Option<String>> {
    let (_, derived) = files
        .get("ro-crate-metadata.json")
        .ok_or_else(|| failed("ARC conversion omitted its RO-Crate"))?;
    let derived: Value = serde_json::from_slice(derived)?;
    // Plain RO-Crate files hold no workbooks, so client workbooks stay as they are.
    let plain = !files.contains_key(INVESTIGATION);
    let current = if plain {
        None
    } else {
        export(directory, main).await.ok()
    };
    let same = plain
        || current
            .as_ref()
            .and_then(|value| value.get("rocrate"))
            .is_some_and(|value| entities(value) == entities(&derived));
    let mut overlay = Files::new();
    for (path, entry) in files {
        let isa = workbook(path) || path == "LICENSE";
        let derived = pointers.contains(path)
            || matches!(
                path.as_str(),
                "ro-crate-metadata.json" | "aruna-metadata.json"
            );
        if (isa && same) || !(isa || derived) {
            continue;
        }
        let existing = command(directory, &["show", &format!("{main}:{path}")])
            .await
            .ok();
        // A plain RO-Crate keeps its client files at the paths its entities name.
        if plain && pointers.contains(path) && existing.is_some() {
            continue;
        }
        if existing.as_deref() != Some(entry.1.as_slice()) {
            overlay.insert(path.clone(), entry.clone());
        }
    }
    if !pointers.is_empty()
        && let Some((mode, generated)) = files.get(".gitattributes")
    {
        // Keep the client's LFS rules and add the pointer paths it lacks.
        let existing = command(directory, &["show", &format!("{main}:.gitattributes")])
            .await
            .unwrap_or_default();
        let mut merged = String::from_utf8_lossy(&existing).into_owned();
        let known: std::collections::BTreeSet<String> = merged.lines().map(str::to_owned).collect();
        for line in String::from_utf8_lossy(generated).lines() {
            if !known.contains(line) {
                if !merged.is_empty() && !merged.ends_with('\n') {
                    merged.push('\n');
                }
                merged.push_str(line);
                merged.push('\n');
            }
        }
        if merged.as_bytes() != existing.as_ref() {
            overlay.insert(".gitattributes".into(), (mode.clone(), merged.into_bytes()));
        }
    }
    let mut removed = Vec::new();
    // A plain snapshot onto an ARC main drops the ISA workbooks and the ARC graph file.
    let switching = plain && crate::rocrate::layout(directory, main).await? == Layout::Arc;
    if !same || switching {
        let listing = command(directory, &["ls-tree", "-r", "-z", "--name-only", main]).await?;
        for path in listing.split(|byte| *byte == 0) {
            let path = std::str::from_utf8(path).map_err(std::io::Error::other)?;
            let generated = workbook(path) || (switching && path == ARUNA_FILE);
            if generated && !path.is_empty() && !files.contains_key(path) {
                removed.push(path.to_string());
            }
        }
    }
    if switching {
        // ARC pointers to objects the plain copy now places elsewhere would become extra files.
        let placed: std::collections::BTreeSet<String> = pointers
            .iter()
            .filter_map(|path| LfsObject::from_pointer(&files.get(path)?.1))
            .map(|object| object.oid)
            .collect();
        for (path, object) in crate::rocrate::pointer_files(directory, main).await? {
            if placed.contains(&object.oid) && !files.contains_key(&path) {
                removed.push(path);
            }
        }
    }
    if overlay.is_empty() && removed.is_empty() {
        return Ok(None);
    }
    write_tree(directory, Some(main), &overlay, &removed)
        .await
        .map(Some)
}

pub(crate) type Pointers = std::collections::BTreeSet<String>;

/// Converts the snapshot's metadata into ARC files. Data files the metadata needs come
/// from `base`. Unrepresentable metadata is an `Err` value.
async fn arc_files(
    directory: &Path,
    source: (Ulid, &str, &[LinkedObject]),
    base: Option<&str>,
) -> std::io::Result<Result<(Files, Pointers), String>> {
    let (document_id, jsonld, objects) = source;
    let objects: serde_json::Map<String, Value> = objects
        .iter()
        .map(|linked| {
            let object = &linked.object;
            let target = json!({"oid": object.sha256, "size": object.size, "key": object.key});
            (linked.entity.clone(), target)
        })
        .collect();
    let conversion = convert(
        json!({"mode":"generate", "document_id":document_id.to_string(),
        "jsonld":jsonld, "objects": objects}),
    )
    .await?;
    let pointers: Pointers = conversion["pointers"]
        .as_object()
        .map(|pointers| pointers.keys().cloned().collect())
        .unwrap_or_default();
    if let Some(error) = conversion["error"].as_str() {
        return Ok(Err(error.into()));
    }
    let mut files = Files::new();
    for (path, content) in conversion["files"]
        .as_object()
        .ok_or_else(|| failed("ARC conversion omitted files"))?
    {
        let data = STANDARD
            .decode(content.as_str().ok_or_else(|| failed("invalid ARC file"))?)
            .map_err(std::io::Error::other)?;
        files.insert(path.clone(), ("100644".into(), data));
    }
    let mut total: usize = files.values().map(|(_, data)| data.len()).sum();
    for path in conversion["required"].as_array().into_iter().flatten() {
        let path = path
            .as_str()
            .ok_or_else(|| failed("invalid ARC data path"))?;
        if files.contains_key(path) {
            continue;
        }
        let data = match base {
            Some(base) => command(directory, &["show", &format!("{base}:{path}")])
                .await
                .ok(),
            None => None,
        };
        let (Some(base), Some(data)) = (base, data) else {
            return Ok(Err(
                "Referenced ARC data is missing; upload it through native Git/LFS".into(),
            ));
        };
        total = total.saturating_add(data.len());
        if total > MAX_GIT_BYTES / 2 || files.len() >= 9999 {
            return Ok(Err(
                "ARC Git files exceed limit; use LFS for large data".into()
            ));
        }
        let entry = command(
            directory,
            &["ls-tree", base, "--", &format!(":(literal){path}")],
        )
        .await?;
        let entry = std::str::from_utf8(&entry).map_err(std::io::Error::other)?;
        let mode = entry.split_whitespace().next().unwrap_or("");
        if !matches!(mode, "100644" | "100755") {
            return Err(failed("unsupported ARC data mode"));
        }
        files.insert(path.to_string(), (mode.to_string(), data.to_vec()));
    }
    Ok(Ok((files, pointers)))
}

/// Snapshot files in the layout the metadata asks for. Metadata marked as an ARC that cannot
/// become one falls back to a plain RO-Crate unless `base` is an ARC already.
async fn snapshot_files(
    directory: &Path,
    source: (Ulid, &str, &[LinkedObject]),
    base: Option<&str>,
) -> std::io::Result<Result<(Files, Pointers), String>> {
    let (_, jsonld, objects) = source;
    let value: Value = serde_json::from_str(jsonld)?;
    if metadata_layout(&value) == Layout::RoCrate {
        return crate::rocrate::files(jsonld, objects).map(Ok);
    }
    let layout = match base {
        Some(base) => Some(crate::rocrate::layout(directory, base).await?),
        None => None,
    };
    Ok(match (arc_files(directory, source, base).await?, layout) {
        (Err(error), None | Some(Layout::RoCrate)) => {
            tracing::info!(%error, "Metadata is not an ARC; the snapshot is a plain RO-Crate");
            Ok(crate::rocrate::files(jsonld, objects)?)
        }
        (converted, _) => converted,
    })
}

/// The author's message, or the default one, followed by the revision `trailer`.
fn snapshot_message(message: Option<&str>, trailer: &str) -> String {
    let subject = message.unwrap_or("feat: capture Aruna metadata");
    format!("{subject}{trailer}")
}

/// Builds the `aruna` commit for `source` and, when main must follow, the main commit.
/// No ref moves: refs change only through replicated records. Unrepresentable metadata is an
/// `Err` value, never a fabricated commit.
pub async fn generate(
    directory: &Path,
    source: GitSnapshot,
    refs: &Refs,
) -> std::io::Result<Result<(String, Option<String>), String>> {
    let main = refs.get("refs/heads/main").cloned();
    let converted = (
        source.document_id,
        source.jsonld.as_str(),
        source.objects.as_slice(),
    );
    let (files, pointers) = match snapshot_files(directory, converted, main.as_deref()).await? {
        Ok(converted) => converted,
        Err(error) => return Ok(Err(error)),
    };
    snapshot_commit(directory, &source, refs, (files, pointers))
        .await
        .map(Ok)
}

/// Commits `files` on `aruna` and, when main must follow, merges them into main.
pub(crate) async fn snapshot_commit(
    directory: &Path,
    source: &GitSnapshot,
    refs: &Refs,
    (files, pointers): (Files, Pointers),
) -> std::io::Result<(String, Option<String>)> {
    let previous = refs.get("refs/heads/aruna").cloned();
    let main = refs.get("refs/heads/main").cloned();
    let trailer = format!("\n\nAruna-Revision: {}\n", source.event_id);
    let tree = write_tree(directory, None, &files, &[]).await?;
    if let Some(previous) = &previous {
        let unchanged = command(directory, &["rev-parse", &format!("{previous}^{{tree}}")]).await?;
        if std::str::from_utf8(&unchanged)
            .map_err(std::io::Error::other)?
            .trim()
            == tree
        {
            return Ok((previous.clone(), None));
        }
    }
    let parents: Vec<&str> = previous.iter().map(String::as_str).collect();
    let message = snapshot_message(source.message.as_deref(), &trailer);
    let commit = commit_tree(directory, &tree, &parents, message, source.occurred_at_ms).await?;
    let main = match main.as_deref() {
        None => Some(commit.clone()),
        Some(main) if previous.as_deref() == Some(main) => Some(commit.clone()),
        Some(main) => match reconcile(directory, main, &files, &pointers).await? {
            Some(tree) => {
                let message = format!("Merge Aruna metadata into main{trailer}");
                let parents = [main, commit.as_str()];
                Some(commit_tree(directory, &tree, &parents, message, source.occurred_at_ms).await?)
            }
            None => None,
        },
    };
    Ok((commit, main))
}

/// Commits the snapshot's metadata on top of `head`, keeping client files and equivalent
/// workbooks. Returns `head` itself when nothing changes.
pub async fn edit(
    directory: &Path,
    head: &str,
    source: GitSnapshot,
    message: String,
) -> std::io::Result<Result<String, String>> {
    let converted = (
        source.document_id,
        source.jsonld.as_str(),
        source.objects.as_slice(),
    );
    let (files, pointers) = match snapshot_files(directory, converted, Some(head)).await? {
        Ok(converted) => converted,
        Err(error) => return Ok(Err(error)),
    };
    Ok(Ok(
        match reconcile(directory, head, &files, &pointers).await? {
            Some(tree) => {
                commit_tree(directory, &tree, &[head], message, source.occurred_at_ms).await?
            }
            None => head.to_string(),
        },
    ))
}

async fn metadata_file(directory: &Path, revision: &str) -> Option<String> {
    let bytes = command(
        directory,
        &["show", &format!("{revision}:aruna-metadata.json")],
    )
    .await
    .ok()?;
    String::from_utf8(bytes.to_vec()).ok()
}

/// Applies the ISA and `aruna-metadata.json` values changed from `old` to `new` onto the
/// `graph` JSON-LD. Returns the merged JSON-LD, or `None` when the graph stays the same.
pub async fn merge_metadata(
    directory: &Path,
    (old, new): (Option<&str>, &str),
    graph: &str,
    location: Option<&DatasetLocation>,
) -> std::io::Result<Result<Option<String>, String>> {
    if crate::rocrate::layout(directory, new).await? == Layout::RoCrate {
        return crate::rocrate::merge_metadata(directory, (old, new), graph, location).await;
    }
    let derived = export(directory, new).await?;
    if let Some(error) = derived["error"].as_str() {
        return Ok(Err(error.into()));
    }
    let (mut base, mut json_base) = (Value::Null, None);
    if let Some(old) = old {
        base = export(directory, old)
            .await
            .ok()
            .and_then(|value| value.get("rocrate").cloned())
            .unwrap_or(Value::Null);
        json_base = metadata_file(directory, old).await;
    }
    let merged = convert(json!({"mode": "merge", "graph": graph, "base": base,
        "new": derived["rocrate"], "json_base": json_base,
        "json_new": metadata_file(directory, new).await}))
    .await?;
    if let Some(error) = merged["error"].as_str() {
        return Ok(Err(error.into()));
    }
    Ok(Ok(merged["jsonld"].as_str().map(str::to_owned)))
}

fn metadata(path: &str) -> bool {
    workbook(path) || matches!(path, "ro-crate-metadata.json" | "aruna-metadata.json")
}

/// Merges `source` into `target`. Conflicting metadata files are
/// resolved by merging the metadata and regenerating them; other conflicts are returned.
pub async fn merge(
    directory: &Path,
    document_id: Ulid,
    target: &str,
    source: &str,
    message: String,
    occurred_at_ms: u64,
) -> std::io::Result<MergeOutcome> {
    let ancestor = async |first: &str, second: &str| {
        command(directory, &["merge-base", "--is-ancestor", first, second])
            .await
            .is_ok()
    };
    if ancestor(source, target).await {
        return Ok(MergeOutcome::UpToDate);
    }
    if ancestor(target, source).await {
        return Ok(MergeOutcome::FastForward);
    }
    let mut process = Command::new("git");
    process
        .current_dir(directory)
        .args([
            "merge-tree",
            "--write-tree",
            "-z",
            "--name-only",
            "--no-messages",
        ])
        .args([target, source])
        .stdin(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true);
    let output = tokio::time::timeout(std::time::Duration::from_secs(300), process.output())
        .await
        .map_err(std::io::Error::other)??;
    // Exit status 1 reports conflicts; anything else is a failure.
    if !matches!(output.status.code(), Some(0 | 1)) {
        return Err(failed("Git merge failed"));
    }
    let mut fields = output.stdout.split(|byte| *byte == 0);
    let mut tree = std::str::from_utf8(fields.next().unwrap_or_default())
        .map_err(std::io::Error::other)?
        .trim()
        .to_string();
    let mut conflicts = std::collections::BTreeSet::new();
    for path in fields.take_while(|path| !path.is_empty()) {
        conflicts.insert(String::from_utf8(path.to_vec()).map_err(std::io::Error::other)?);
    }
    let (resolvable, other): (Vec<String>, Vec<String>) =
        conflicts.into_iter().partition(|path| metadata(path));
    if !other.is_empty() {
        return Ok(MergeOutcome::Conflicts(other));
    }
    if !resolvable.is_empty() {
        // A plain RO-Crate keeps its whole graph in its RO-Crate file.
        let graph = match crate::rocrate::layout(directory, target).await? {
            Layout::Arc => metadata_file(directory, target).await,
            Layout::RoCrate => command(directory, &["show", &format!("{target}:{CRATE_FILE}")])
                .await
                .ok()
                .and_then(|bytes| String::from_utf8(bytes.to_vec()).ok()),
        };
        let (Some(base), Some(graph)) = (
            crate::repo::merge_base(directory, target, source).await,
            graph,
        ) else {
            return Ok(MergeOutcome::Conflicts(resolvable));
        };
        let merged = match merge_metadata(directory, (Some(&base), source), &graph, None).await? {
            Ok(merged) => merged.unwrap_or(graph),
            Err(error) => return Ok(MergeOutcome::Failed(error)),
        };
        let converted = (document_id, merged.as_str(), &[][..]);
        let (files, pointers) = match snapshot_files(directory, converted, Some(target)).await? {
            Ok(converted) => converted,
            Err(error) => return Ok(MergeOutcome::Failed(error)),
        };
        if let Some(resolved) = reconcile(directory, &tree, &files, &pointers).await? {
            tree = resolved;
        }
    }
    let parents = [target, source];
    let commit = commit_tree(directory, &tree, &parents, message, occurred_at_ms).await?;
    Ok(MergeOutcome::Merged(commit))
}

#[cfg(test)]
mod tests {
    use super::snapshot_message;

    #[test]
    fn keeps_default_message() {
        let trailer = "\n\nAruna-Revision: 01M\n";
        assert_eq!(
            snapshot_message(None, trailer),
            "feat: capture Aruna metadata\n\nAruna-Revision: 01M\n"
        );
    }

    #[test]
    fn uses_author_message() {
        let trailer = "\n\nAruna-Revision: 01M\n";
        assert_eq!(
            snapshot_message(Some("Add run 42\n\nNew LC-MS data"), trailer),
            "Add run 42\n\nNew LC-MS data\n\nAruna-Revision: 01M\n"
        );
    }
}
