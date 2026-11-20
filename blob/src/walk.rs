//! Walks a source without a provider cursor depth first in name order, a directory before its
//! entries, so a page continues after the last path it listed and lists only what follows it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::errors::StagingSourceError;
use aruna_core::structs::execution::source_access::{ListCursor, ListRequest, SourceEntry};
use std::cmp::Ordering;
use std::collections::VecDeque;

/// One child of a listed directory and what its source knows about it.
pub(crate) struct Child<T> {
    pub name: String,
    /// Whether a recursive walk enters it.
    pub is_dir: bool,
    pub value: T,
}

/// What a walk needs next.
pub(crate) enum Step<T> {
    /// The children of this directory path, handed back through [`Walk::listed`].
    List(String),
    /// The next path in walk order.
    Entry { path: String, child: Child<T> },
    Done,
}

/// A walk below a base path that starts after a cursor path. A page lists the directories on
/// the cursor path and the ones after it, never the subtrees before it.
pub(crate) struct Walk<T> {
    recursive: bool,
    /// The directory to list next and the cursor names still to pass inside it.
    pending: Option<(String, Vec<String>)>,
    frames: Vec<Frame<T>>,
}

struct Frame<T> {
    path: String,
    children: VecDeque<Child<T>>,
    after: Vec<String>,
}

impl<T> Walk<T> {
    /// A walk of `base` that resumes after the path the names in `after` spell below it.
    pub(crate) fn new(base: &str, after: Vec<String>, recursive: bool) -> Self {
        Self {
            recursive,
            pending: Some((base.trim_matches('/').to_string(), after)),
            frames: Vec::new(),
        }
    }

    pub(crate) fn step(&mut self) -> Step<T> {
        if let Some((path, _)) = &self.pending {
            return Step::List(path.clone());
        }
        while let Some(frame) = self.frames.last_mut() {
            let Some(child) = frame.children.pop_front() else {
                self.frames.pop();
                continue;
            };
            let path = join_path(&frame.path, &child.name);
            let enter = child.is_dir && self.recursive;
            let order = (frame.after.first()).map(|first| child.name.as_str().cmp(first.as_str()));
            match order {
                Some(Ordering::Less) => {}
                // An earlier page listed it; only its subtree may hold paths after the cursor.
                Some(Ordering::Equal) => {
                    if enter {
                        self.pending = Some((path.clone(), frame.after[1..].to_vec()));
                        return Step::List(path);
                    }
                }
                Some(Ordering::Greater) | None => {
                    if enter {
                        self.pending = Some((path.clone(), Vec::new()));
                    }
                    return Step::Entry { path, child };
                }
            }
        }
        Step::Done
    }

    /// Takes the children of the directory the last [`Step::List`] asked for.
    pub(crate) fn listed(&mut self, mut children: Vec<Child<T>>) {
        let Some((path, after)) = self.pending.take() else {
            return;
        };
        children.sort_by(|left, right| left.name.cmp(&right.name));
        self.frames.push(Frame {
            path,
            children: children.into(),
            after,
        });
    }
}

/// The names below `base` of the path a walk cursor continues after. Key bounds and the other
/// cursor kinds belong to sources with a provider cursor.
pub(crate) fn walk_after(
    request: &ListRequest,
    base: &str,
) -> Result<Vec<String>, StagingSourceError> {
    if request.lower.is_some() || request.stop.is_some() {
        return Err(StagingSourceError::InvalidListing(
            "key bounds need a source with a provider cursor".to_string(),
        ));
    }
    let path = match &request.cursor {
        None => return Ok(Vec::new()),
        Some(ListCursor::Walk(path)) => path.trim_matches('/'),
        Some(ListCursor::After(_) | ListCursor::At(_) | ListCursor::Offset(_)) => {
            return Err(StagingSourceError::InvalidListing(
                "this source continues after a walked path, not by key or offset".to_string(),
            ));
        }
    };
    let base = base.trim_matches('/');
    let below = match base.is_empty() {
        true => Some(path),
        false => path.strip_prefix(base).and_then(|rest| rest.strip_prefix('/')),
    };
    let below = below.filter(|rest| !rest.is_empty()).ok_or_else(|| {
        StagingSourceError::InvalidListing("the cursor lies outside the listed path".to_string())
    })?;
    Ok(below.split('/').map(ToOwned::to_owned).collect())
}

/// A walked page and the cursor after its last path when more follows.
pub(crate) fn walk_page(
    entries: Vec<SourceEntry>,
    truncated: bool,
) -> (Vec<SourceEntry>, Option<ListCursor>) {
    let next = (entries.last())
        .filter(|_| truncated)
        .map(|entry| ListCursor::Walk(entry.path.clone()));
    (entries, next)
}

fn join_path(directory: &str, name: &str) -> String {
    match directory.is_empty() {
        true => name.to_string(),
        false => format!("{directory}/{name}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A tree as paths; a path ending in `/` is a directory.
    const TREE: [&str; 7] = ["b.txt", "a/", "a/z.txt", "a/m/", "a/m/x.txt", "a-c", "c/"];

    fn children(directory: &str) -> Vec<Child<()>> {
        let prefix = match directory.is_empty() {
            true => String::new(),
            false => format!("{directory}/"),
        };
        (TREE.iter())
            .filter_map(|path| path.strip_prefix(&prefix))
            .filter(|rest| !rest.trim_end_matches('/').contains('/') && !rest.is_empty())
            .map(|rest| Child {
                name: rest.trim_end_matches('/').to_string(),
                is_dir: rest.ends_with('/'),
                value: (),
            })
            .collect()
    }

    /// The paths a walk gives, and the directories it listed.
    fn walked(after: &[&str], recursive: bool) -> (Vec<String>, Vec<String>) {
        let after = after.iter().map(ToString::to_string).collect();
        let mut walk = Walk::new("", after, recursive);
        let (mut paths, mut listed) = (Vec::new(), Vec::new());
        loop {
            match walk.step() {
                Step::List(directory) => {
                    walk.listed(children(&directory));
                    listed.push(directory);
                }
                Step::Entry { path, .. } => paths.push(path),
                Step::Done => return (paths, listed),
            }
        }
    }

    #[test]
    fn walks_name_order() {
        let (paths, _) = walked(&[], true);
        assert_eq!(paths, ["a", "a/m", "a/m/x.txt", "a/z.txt", "a-c", "b.txt", "c"]);
        let (paths, _) = walked(&[], false);
        assert_eq!(paths, ["a", "a-c", "b.txt", "c"]);
    }

    // A resumed walk lists the directories on the cursor path, never the subtrees before it.
    #[test]
    fn resumes_after_cursor() {
        let (paths, listed) = walked(&["a", "m", "x.txt"], true);
        assert_eq!(paths, ["a/z.txt", "a-c", "b.txt", "c"]);
        assert_eq!(listed, ["", "a", "a/m", "c"]);
        let (paths, listed) = walked(&["a-c"], true);
        assert_eq!(paths, ["b.txt", "c"]);
        assert_eq!(listed, ["", "c"]);
        let (paths, _) = walked(&["a"], true);
        assert_eq!(paths, ["a/m", "a/m/x.txt", "a/z.txt", "a-c", "b.txt", "c"]);
    }

    #[test]
    fn cursor_names_checked() {
        let mut request = ListRequest {
            cursor: Some(ListCursor::Walk("data/a/b.txt".to_string())),
            ..ListRequest::default()
        };
        assert_eq!(walk_after(&request, "data/").unwrap(), ["a", "b.txt"]);
        assert!(walk_after(&request, "dat").is_err());
        request.cursor = Some(ListCursor::After("data/a".to_string()));
        assert!(walk_after(&request, "data").is_err());
    }
}
