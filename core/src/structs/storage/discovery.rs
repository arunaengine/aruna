//! Places files a backend import finds without a recorded mapping, by file name patterns.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::errors::ConversionError;
use crate::structs::storage::backend_import::{BackendImport, PathIdentity};
use crate::structs::storage::glob::{check_glob, glob_match, globs_overlap};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

/// Patterns one import rule may hold.
pub const MAX_DISCOVERY_RULES: usize = 32;

/// Characters one pattern may hold, which bounds every match and overlap check.
pub const MAX_PATTERN_LEN: usize = 128;

/// Sends an unmapped file whose name matches the glob `pattern` (`*`, `?`, `[0-9]`, `{2,8}`,
/// `{fna,fa}`) to `folder` of the bucket; `lars/{name}` takes the files below that storage
/// folder instead. Uploads below `folder` go back the same way.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DiscoveryRule {
    pub pattern: String,
    pub folder: String,
}

impl DiscoveryRule {
    /// The storage folder of a `<folder>/{name}` pattern; `None` for a file name pattern.
    pub fn storage_folder(&self) -> Option<&str> {
        let folder = self.pattern.strip_suffix("{name}")?;
        folder.ends_with('/').then_some(folder)
    }
}

/// The discovery rules of one import rule, keyed like the rule itself.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct DiscoveryRules {
    pub rules: Vec<DiscoveryRule>,
}

impl DiscoveryRules {
    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

/// Where an unmapped file goes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Placement {
    /// No pattern matches: the rule's own folder, as without discovery rules.
    Root,
    /// Exactly one pattern matches.
    Folder(String),
    /// Several patterns match; such a file is reported and not imported.
    Conflict(Vec<String>),
}

/// Checks the count, each pattern and that no two patterns can match one name.
/// Folders are checked by the caller.
pub fn check_patterns(rules: &[DiscoveryRule]) -> Result<(), String> {
    if rules.len() > MAX_DISCOVERY_RULES {
        return Err(format!(
            "at most {MAX_DISCOVERY_RULES} patterns are allowed"
        ));
    }
    for (index, rule) in rules.iter().enumerate() {
        let pattern = &rule.pattern;
        if pattern.is_empty() || pattern.chars().count() > MAX_PATTERN_LEN {
            return Err(format!("a pattern needs 1 to {MAX_PATTERN_LEN} characters"));
        }
        if let Some(folder) = rule.storage_folder() {
            check_folder(folder)?;
            let nested = rules[..index]
                .iter()
                .filter_map(DiscoveryRule::storage_folder)
                .find(|other| other.starts_with(folder) || folder.starts_with(other));
            if let Some(other) = nested {
                return Err(format!("storage folders `{other}` and `{folder}` overlap"));
            }
            continue;
        }
        if pattern.contains('/') || pattern.chars().any(char::is_control) {
            return Err(format!(
                "pattern `{pattern}` must not hold `/` or control characters"
            ));
        }
        check_glob(pattern)?;
        if let Some(other) = rules[..index]
            .iter()
            .filter(|other| other.storage_folder().is_none())
            .find(|other| globs_overlap(&other.pattern, pattern))
        {
            return Err(format!(
                "patterns `{}` and `{pattern}` can match the same file",
                other.pattern
            ));
        }
    }
    Ok(())
}

/// A storage folder of a `<folder>/{name}` pattern: relative, ending with `/`, without
/// `.` or `..` parts, braces or glob characters.
fn check_folder(folder: &str) -> Result<(), String> {
    let parts_ok = folder[..folder.len() - 1]
        .split('/')
        .all(|part| !part.is_empty() && part != "." && part != ".." && part.trim() == part);
    let plain = !folder.contains(['{', '}', '*', '?', '[', ']']);
    match parts_ok && plain && !folder.chars().any(char::is_control) {
        true => Ok(()),
        false => Err(format!(
            "`{folder}{{name}}` needs a relative storage folder before {{name}}"
        )),
    }
}

/// Places a file by its name; overlapping stored rules still give a conflict, never a guess.
pub fn place(rules: &[DiscoveryRule], name: &str) -> Placement {
    let matched: Vec<&DiscoveryRule> = rules
        .iter()
        .filter(|rule| rule.storage_folder().is_none() && glob_match(&rule.pattern, name))
        .collect();
    match matched.as_slice() {
        [] => Placement::Root,
        [rule] => Placement::Folder(rule.folder.clone()),
        _ => Placement::Conflict(matched.iter().map(|rule| rule.pattern.clone()).collect()),
    }
}

/// The identity a run gives a backend file without a recorded mapping or write intent.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileIdentity {
    /// The bucket key: the discovery folder, if one pattern matches, then the path below
    /// `backend_prefix`. It never depends on the rule's `key_prefix`.
    pub key: String,
    /// The version id the write pattern encodes in the file name, kept apart from the key.
    pub version_id: Option<Ulid>,
    /// Whether a pattern placed the file; `false` means the bucket root.
    pub matched: bool,
}

/// Why a file gets no identity; such a file is reported and not imported.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IdentityError {
    /// The name fits more than one key or version of the rule's patterns.
    Ambiguous,
    /// Several discovery patterns match the name.
    Conflict(Vec<String>),
}

/// Places a file by its name alone. A name the current or an `earlier` write pattern encodes
/// is matched without its version id; a folder the pattern stripped stays unknown, so the
/// file goes to the bucket root or the matching folder. `None` outside `backend_prefix`.
pub fn file_identity(
    rule: &BackendImport,
    earlier: &[String],
    rules: &[DiscoveryRule],
    backend_key: &str,
) -> Option<Result<FileIdentity, IdentityError>> {
    let rest = backend_key.strip_prefix(&rule.backend_prefix)?;
    let file = rest.rsplit('/').next().unwrap_or(rest);
    let (path, name, version_id) = match rule.decode_any(backend_key, earlier) {
        PathIdentity::Ambiguous => return Some(Err(IdentityError::Ambiguous)),
        PathIdentity::Decoded { key, name, id } => {
            let path = key
                .and_then(|key| key.strip_prefix(&rule.key_prefix).map(str::to_string))
                .unwrap_or_else(|| name.clone());
            (path, name, Some(id))
        }
        PathIdentity::Unmatched => (
            rest.to_string(),
            listed_name(rule, earlier, file).to_string(),
            None,
        ),
    };
    // A storage folder pattern takes its files before any file name pattern.
    let folder = rules.iter().find_map(|rule| {
        let rest = path.strip_prefix(rule.storage_folder()?)?;
        Some(format!("{}{rest}", rule.folder))
    });
    if let Some(key) = folder {
        return Some(Ok(FileIdentity {
            key,
            version_id,
            matched: true,
        }));
    }
    Some(match place(rules, &name) {
        Placement::Root => Ok(FileIdentity {
            key: path,
            version_id,
            matched: false,
        }),
        Placement::Folder(folder) => Ok(FileIdentity {
            key: format!("{folder}{path}"),
            version_id,
            matched: true,
        }),
        Placement::Conflict(patterns) => Err(IdentityError::Conflict(patterns)),
    })
}

/// A storage folder pattern needs a write pattern with `{dir}`, which keeps the storage folder.
pub fn check_naming(rules: &[DiscoveryRule], write_pattern: &str) -> Result<(), String> {
    let folders = rules.iter().any(|rule| rule.storage_folder().is_some());
    match folders && !write_pattern.contains("{dir}") {
        true => Err("a `<folder>/{name}` mapping needs a naming with `{dir}`".to_string()),
        false => Ok(()),
    }
}

/// Where an upload to `path`, relative to the rule's `key_prefix`, goes below the rule's
/// folder: a storage folder pattern swaps its Aruna folder for the storage folder, and a file
/// name pattern drops its Aruna folder when the name matches. Other paths stay.
pub fn storage_path(rules: &[DiscoveryRule], path: &str) -> String {
    let name = path.rsplit('/').next().unwrap_or(path);
    for rule in rules {
        let Some(rest) = path.strip_prefix(&rule.folder) else {
            continue;
        };
        match rule.storage_folder() {
            Some(folder) => return format!("{folder}{rest}"),
            None if !rest.is_empty() && glob_match(&rule.pattern, name) => return rest.to_string(),
            None => {}
        }
    }
    path.to_string()
}

/// The file name patterns see: without the version id that the rule's current or an earlier
/// write pattern, or a same-name tag, adds.
pub fn listed_name<'a>(rule: &BackendImport, earlier: &[String], name: &'a str) -> &'a str {
    let name = strip_id(name, "~", "").unwrap_or(name);
    let patterns =
        std::iter::once(rule.write_pattern.as_str()).chain(earlier.iter().map(String::as_str));
    patterns
        .map(|pattern| strip_wrapper(pattern, name))
        .find(|stripped| *stripped != name)
        .unwrap_or(name)
}

/// `name` without the version id wrapper of `pattern`.
fn strip_wrapper<'a>(pattern: &str, name: &'a str) -> &'a str {
    let Some(at) = pattern.find("{name}") else {
        return name;
    };
    let head = &pattern[..at];
    let start = [
        head.rfind('/').map(|slash| slash + 1),
        head.rfind("{dir}").map(|dir| dir + 5),
    ]
    .into_iter()
    .flatten()
    .max()
    .unwrap_or(0);
    let (before, after) = (&head[start..], &pattern[at + "{name}".len()..]);
    match (before.split_once("{id}"), after.split_once("{id}")) {
        (Some((lead, tail)), None) if after.is_empty() => {
            strip_prefix_id(name, lead, tail).unwrap_or(name)
        }
        (None, Some((lead, tail))) if before.is_empty() && !after.contains('/') => {
            strip_id(name, lead, tail).unwrap_or(name)
        }
        _ => name,
    }
}

/// `name` without a trailing `{lead}<id>{tail}`.
fn strip_id<'a>(name: &'a str, lead: &str, tail: &str) -> Option<&'a str> {
    let rest = name.strip_suffix(tail)?;
    let id_at = rest.len().checked_sub(26)?;
    let (rest, id) = (rest.get(..id_at)?, rest.get(id_at..)?);
    Ulid::from_string(id).ok()?;
    rest.strip_suffix(lead).filter(|base| !base.is_empty())
}

/// `name` without a leading `{lead}<id>{tail}`.
fn strip_prefix_id<'a>(name: &'a str, lead: &str, tail: &str) -> Option<&'a str> {
    let rest = name.strip_prefix(lead)?;
    let (id, rest) = (rest.get(..26)?, rest.get(26..)?);
    Ulid::from_string(id).ok()?;
    rest.strip_prefix(tail).filter(|base| !base.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::SystemTime;

    fn rule(pattern: &str, folder: &str) -> DiscoveryRule {
        DiscoveryRule {
            pattern: pattern.to_string(),
            folder: folder.to_string(),
        }
    }

    fn import(write_pattern: &str) -> BackendImport {
        BackendImport {
            import_id: Ulid::nil(),
            group_id: Ulid::nil(),
            backend_id: Ulid::nil(),
            backend_prefix: "raw/".to_string(),
            bucket: "bucket".to_string(),
            key_prefix: String::new(),
            schedule: None,
            created_by: Default::default(),
            created_at: SystemTime::UNIX_EPOCH,
            updated_at: SystemTime::UNIX_EPOCH,
            last_job: None,
            last_started_ms: None,
            write_backend_id: None,
            write_prefix: None,
            write_pattern: write_pattern.to_string(),
        }
    }

    fn genome() -> Vec<DiscoveryRule> {
        vec![
            rule("*.faa.gz", "proteins/"),
            rule("*.fna.gz", "nucleotides/"),
        ]
    }

    #[test]
    fn matches_globs() {
        assert!(glob_match("*.faa.gz", "sample.faa.gz"));
        assert!(glob_match("*.faa.gz", ".faa.gz"));
        assert!(!glob_match("*.faa.gz", "sample.faa.gz.bak"));
        assert!(glob_match("s?mple*", "sample.txt"));
        assert!(!glob_match("s?mple", "smple"));
        assert!(glob_match("a*b*c", "a-b-b-c"));
        assert!(!glob_match("a*b*c", "a-c-b"));
        // Dots are literal; brackets make a set.
        assert!(glob_match("[x].txt", "x.txt"));
        assert!(!glob_match("[x].txt", "[x].txt"));
    }

    #[test]
    fn detects_overlaps() {
        assert!(!globs_overlap("*.faa.gz", "*.fna.gz"));
        assert!(globs_overlap("*.gz", "*.faa.gz"));
        assert!(globs_overlap("sample*", "*.faa.gz"));
        assert!(globs_overlap("a?c", "*b*"));
        assert!(!globs_overlap("a?c", "ab"));
        assert!(!globs_overlap("a*", "b*"));
        assert!(globs_overlap("*", "x"));
        assert!(globs_overlap("a*b", "a**b"));
    }

    #[test]
    fn checks_patterns() {
        assert_eq!(check_patterns(&genome()), Ok(()));
        let overlapping = [rule("*.gz", "a/"), rule("*.faa.gz", "b/")];
        assert!(check_patterns(&overlapping).is_err());
        assert!(check_patterns(&[rule("raw/*.gz", "a/")]).is_err());
        assert!(check_patterns(&[rule("", "a/")]).is_err());
        let long = "x".repeat(MAX_PATTERN_LEN + 1);
        assert!(check_patterns(&[rule(&long, "a/")]).is_err());
        let many: Vec<_> = (0..=MAX_DISCOVERY_RULES)
            .map(|index| rule(&format!("{index}.x"), "a/"))
            .collect();
        assert!(check_patterns(&many).is_err());
    }

    #[test]
    fn places_one_match() {
        assert_eq!(place(&genome(), "baz"), Placement::Root);
        assert_eq!(
            place(&genome(), "sample.faa.gz"),
            Placement::Folder("proteins/".to_string())
        );
        // Stored rules that overlap never pick a winner.
        let stored = [rule("*.gz", "a/"), rule("*.faa.gz", "b/")];
        assert_eq!(
            place(&stored, "sample.faa.gz"),
            Placement::Conflict(vec!["*.gz".to_string(), "*.faa.gz".to_string()])
        );
    }

    /// The key and version a file gets, or the error.
    fn identity(
        rule: &BackendImport,
        path: &str,
    ) -> Option<Result<(String, Option<Ulid>), IdentityError>> {
        let found = file_identity(rule, &[], &genome(), path)?;
        Some(found.map(|found| (found.key, found.version_id)))
    }

    #[test]
    fn keeps_nested_paths() {
        let plain = import("{dir}{name}");
        let key = |path: &str| identity(&plain, path);
        assert_eq!(key("raw/baz"), Some(Ok(("baz".to_string(), None))));
        assert_eq!(
            key("raw/run1/lane2/sample.faa.gz"),
            Some(Ok(("proteins/run1/lane2/sample.faa.gz".to_string(), None)))
        );
        assert_eq!(
            key("raw/sample.fna.gz"),
            Some(Ok(("nucleotides/sample.fna.gz".to_string(), None)))
        );
        assert_eq!(key("other/sample.faa.gz"), None);
        // The rule's own folder is for its writes; found files go to the bucket root.
        let mut prefixed = plain.clone();
        prefixed.key_prefix = "in/".to_string();
        let found = |path: &str| identity(&prefixed, path);
        assert_eq!(
            found("raw/a.faa.gz"),
            Some(Ok(("proteins/a.faa.gz".to_string(), None)))
        );
        assert_eq!(found("raw/baz"), Some(Ok(("baz".to_string(), None))));
    }

    #[test]
    fn reads_version_wrappers() {
        let ulid = Ulid::from_parts(1, 2);
        let id = ulid.to_string();
        let mut suffix = import("{dir}{name}~{id}");
        suffix.key_prefix = "in/".to_string();
        let tagged = format!("sample.faa.gz~{id}");
        assert_eq!(listed_name(&suffix, &[], &tagged), "sample.faa.gz");
        // A recovered version keeps its id apart and goes where the rules place its name.
        let path = format!("raw/x/{tagged}");
        let placed = ("proteins/x/sample.faa.gz".to_string(), Some(ulid));
        assert_eq!(identity(&suffix, &path), Some(Ok(placed.clone())));
        // A name an earlier pattern wrote is read without its wrapper too.
        let current = import("{dir}{name}");
        let earlier = ["{dir}{name}~{id}".to_string()];
        let found = file_identity(&current, &earlier, &genome(), &path)
            .unwrap()
            .unwrap();
        assert_eq!(
            (found.key, found.version_id, found.matched),
            (placed.0, placed.1, true)
        );
        let prefix = import("{dir}{id}_{name}");
        assert_eq!(
            listed_name(&prefix, &[], &format!("{id}_a.fna.gz")),
            "a.fna.gz"
        );
        assert_eq!(
            listed_name(
                &current,
                &["{dir}{id}_{name}".to_string()],
                &format!("{id}_a")
            ),
            "a"
        );
        // A same-name tag after a plain pattern is a wrapper too.
        let plain = import("{dir}{name}");
        assert_eq!(listed_name(&plain, &[], &tagged), "sample.faa.gz");
        // Names that only look wrapped stay as they are.
        assert_eq!(
            listed_name(&prefix, &[], "notanid_a.fna.gz"),
            "notanid_a.fna.gz"
        );
        assert_eq!(
            listed_name(&suffix, &[], &format!("~{id}")),
            format!("~{id}")
        );
        assert_eq!(listed_name(&plain, &[], "a~b"), "a~b");
    }
    #[test]
    fn maps_both_ways() {
        let rules = [rule("*.faa", "proteins/"), rule("lars/{name}", "foobar/")];
        // Uploads below an Aruna folder go to the storage path; other paths stay.
        assert_eq!(storage_path(&rules, "proteins/a/x.faa"), "a/x.faa");
        assert_eq!(
            storage_path(&rules, "proteins/readme.txt"),
            "proteins/readme.txt"
        );
        assert_eq!(storage_path(&rules, "foobar/sub/x"), "lars/sub/x");
        assert_eq!(storage_path(&rules, "other/x.faa"), "other/x.faa");
        // Files found in storage come back to the same Aruna paths.
        let found = |path: &str| {
            let backend_key = format!("raw/{path}");
            file_identity(&import("{dir}{name}~{id}"), &[], &rules, &backend_key)
                .unwrap()
                .unwrap()
                .key
        };
        assert_eq!(found("a/x.faa"), "proteins/a/x.faa");
        assert_eq!(found("lars/sub/x.faa"), "foobar/sub/x.faa");
        assert_eq!(found("other/y.txt"), "other/y.txt");
    }
}
