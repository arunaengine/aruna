//! Places files a backend import finds without a recorded mapping, by file name patterns.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::errors::ConversionError;
use crate::structs::storage::backend_import::BackendImport;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

/// Patterns one import rule may hold.
pub const MAX_DISCOVERY_RULES: usize = 32;

/// Characters one pattern may hold, which bounds every match and overlap check.
pub const MAX_PATTERN_LEN: usize = 128;

/// Sends an unmapped file whose name matches `pattern` to `folder` below the rule's
/// `key_prefix`. `*` matches any characters of the name, `?` one; all else is literal.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DiscoveryRule {
    pub pattern: String,
    pub folder: String,
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
        if pattern.contains('/') || pattern.chars().any(char::is_control) {
            return Err(format!(
                "pattern `{pattern}` must not hold `/` or control characters"
            ));
        }
        if let Some(other) = rules[..index]
            .iter()
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

/// Matches the whole name against a pattern of `*`, `?` and literal characters.
pub fn glob_match(pattern: &str, name: &str) -> bool {
    let (pattern, name): (Vec<char>, Vec<char>) =
        (pattern.chars().collect(), name.chars().collect());
    let (mut p, mut n) = (0, 0);
    // The last star and the name position it resumes from.
    let mut star: Option<(usize, usize)> = None;
    while n < name.len() {
        match pattern.get(p) {
            Some('*') => {
                star = Some((p, n));
                p += 1;
            }
            Some(&c) if c == '?' || c == name[n] => {
                p += 1;
                n += 1;
            }
            _ => match star {
                Some((at, from)) => {
                    p = at + 1;
                    n = from + 1;
                    star = Some((at, from + 1));
                }
                None => return false,
            },
        }
    }
    pattern[p..].iter().all(|&c| c == '*')
}

/// Whether some name matches both patterns, by walking both patterns side by side.
pub fn globs_overlap(left: &str, right: &str) -> bool {
    let (a, b): (Vec<char>, Vec<char>) = (left.chars().collect(), right.chars().collect());
    let width = b.len() + 1;
    let mut seen = vec![false; (a.len() + 1) * width];
    let mut stack = vec![(0, 0)];
    while let Some((i, j)) = stack.pop() {
        if std::mem::replace(&mut seen[i * width + j], true) {
            continue;
        }
        if i == a.len() && j == b.len() {
            return true;
        }
        let (x, y) = (a.get(i).copied(), b.get(j).copied());
        if x == Some('*') {
            stack.push((i + 1, j));
        }
        if y == Some('*') {
            stack.push((i, j + 1));
        }
        let (Some(x), Some(y)) = (x, y) else {
            continue;
        };
        // One character both accept; a star stays to accept more.
        let wild = |c: char| c == '*' || c == '?';
        if wild(x) || wild(y) || x == y {
            let next = (i + usize::from(x != '*'), j + usize::from(y != '*'));
            if next != (i, j) {
                stack.push(next);
            }
        }
    }
    false
}

/// Places a file by its name; overlapping stored rules still give a conflict, never a guess.
pub fn place(rules: &[DiscoveryRule], name: &str) -> Placement {
    let matched: Vec<&DiscoveryRule> = rules
        .iter()
        .filter(|rule| glob_match(&rule.pattern, name))
        .collect();
    match matched.as_slice() {
        [] => Placement::Root,
        [rule] => Placement::Folder(rule.folder.clone()),
        _ => Placement::Conflict(matched.iter().map(|rule| rule.pattern.clone()).collect()),
    }
}

/// The bucket key of an unmapped backend key: the discovery folder goes between the rule's
/// `key_prefix` and the path below `backend_prefix`. `None` outside `backend_prefix`.
pub fn discovered_key(
    rule: &BackendImport,
    rules: &[DiscoveryRule],
    backend_key: &str,
) -> Option<Result<String, Vec<String>>> {
    let rest = backend_key.strip_prefix(&rule.backend_prefix)?;
    let name = rest.rsplit('/').next().unwrap_or(rest);
    Some(match place(rules, listed_name(rule, name)) {
        Placement::Root => Ok(format!("{}{rest}", rule.key_prefix)),
        Placement::Folder(folder) => Ok(format!("{}{folder}{rest}", rule.key_prefix)),
        Placement::Conflict(patterns) => Err(patterns),
    })
}

/// The file name patterns see: without the version id that the rule's write pattern or a
/// same-name tag adds. Call point for the shared naming parser of the suffix naming change.
pub fn listed_name<'a>(rule: &BackendImport, name: &'a str) -> &'a str {
    let name = strip_id(name, "~", "").unwrap_or(name);
    let pattern = &rule.write_pattern;
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
        // Brackets and dots are literal.
        assert!(glob_match("[x].txt", "[x].txt"));
        assert!(!glob_match("[x].txt", "x.txt"));
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

    #[test]
    fn keeps_nested_paths() {
        let plain = import("{dir}{name}");
        let key = |path: &str| discovered_key(&plain, &genome(), path);
        assert_eq!(key("raw/baz"), Some(Ok("baz".to_string())));
        assert_eq!(
            key("raw/run1/lane2/sample.faa.gz"),
            Some(Ok("proteins/run1/lane2/sample.faa.gz".to_string()))
        );
        assert_eq!(
            key("raw/sample.fna.gz"),
            Some(Ok("nucleotides/sample.fna.gz".to_string()))
        );
        assert_eq!(key("other/sample.faa.gz"), None);
        let mut prefixed = plain.clone();
        prefixed.key_prefix = "in/".to_string();
        assert_eq!(
            discovered_key(&prefixed, &genome(), "raw/a.faa.gz"),
            Some(Ok("in/proteins/a.faa.gz".to_string()))
        );
    }

    #[test]
    fn reads_version_wrappers() {
        let id = Ulid::from_parts(1, 2).to_string();
        let suffix = import("{dir}{name}~{id}");
        let tagged = format!("sample.faa.gz~{id}");
        assert_eq!(listed_name(&suffix, &tagged), "sample.faa.gz");
        let path = format!("raw/x/{tagged}");
        assert_eq!(
            discovered_key(&suffix, &genome(), &path),
            Some(Ok(format!("proteins/x/{tagged}")))
        );
        let prefix = import("{dir}{id}_{name}");
        assert_eq!(listed_name(&prefix, &format!("{id}_a.fna.gz")), "a.fna.gz");
        // A same-name tag after a plain pattern is a wrapper too.
        let plain = import("{dir}{name}");
        assert_eq!(listed_name(&plain, &tagged), "sample.faa.gz");
        // Names that only look wrapped stay as they are.
        assert_eq!(listed_name(&prefix, "notanid_a.fna.gz"), "notanid_a.fna.gz");
        assert_eq!(listed_name(&suffix, &format!("~{id}")), format!("~{id}"));
        assert_eq!(listed_name(&plain, "a~b"), "a~b");
    }
}
