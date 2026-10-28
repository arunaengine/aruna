//! Lists only what a caller may see: a common prefix shows once an entry below it does, and a
//! cursor names only shown entries, or is sealed when a page stops among hidden ones.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use futures_util::future::BoxFuture;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::driver::{DriverContext, drive};
use crate::s3::object::list::{
    ListBucketError, ListBucketInput, ListBucketOperation, ListContinuationToken, ListedObject,
};
use crate::s3::object::search::{open_position, seal_position, sealed_position};

/// Hidden entries one listing request examines at most. A page that reaches it ends early and
/// its sealed cursor resumes there, so it bounds the work of one request, never its results.
pub const SCAN_BUDGET: usize = 1_000;

/// Starts a sealed cursor; no base64 token of a listing does, and a marker that is a key
/// starting with it stays that key when it does not open.
const SEALED_MARK: char = '~';

/// A listing position sealed with this node's key, so it names no entry; a restart of the
/// node makes it invalid.
pub fn seal_cursor(value: &impl Serialize) -> Option<String> {
    let sealed = seal_position(&postcard::to_allocvec(value).ok()?).ok()?;
    Some(format!("{SEALED_MARK}{}", URL_SAFE_NO_PAD.encode(sealed)))
}

/// Whether `text` claims to be a sealed cursor.
pub fn is_sealed(text: &str) -> bool {
    text.starts_with(SEALED_MARK)
}

/// The position a sealed cursor of this node holds; `None` for any other text.
pub fn open_cursor<T: DeserializeOwned>(text: &str) -> Option<T> {
    let sealed = URL_SAFE_NO_PAD
        .decode(text.strip_prefix(SEALED_MARK)?)
        .ok()?;
    if !sealed_position(&sealed) {
        return None;
    }
    postcard::from_bytes(&open_position(&sealed).ok()?).ok()
}

/// Where a listing stands among hidden entries: after the last examined entry, and inside a
/// common prefix whose shown entry is still sought, with that search's own position.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct HiddenCursor {
    pub position: Option<ListContinuationToken>,
    pub pending: Option<(String, Option<Vec<u8>>)>,
}

/// Where the next page of a listing starts.
#[derive(Clone, Debug, PartialEq)]
pub enum NextPage {
    /// After the last shown entry, a position the caller already saw.
    After(ListContinuationToken),
    /// Among hidden entries, sealed.
    Sealed(String),
}

/// One page of shown entries.
#[derive(Debug, Default)]
pub struct VisiblePage {
    pub objects: Vec<ListedObject>,
    pub prefixes: Vec<String>,
    pub next: Option<NextPage>,
}

#[derive(Debug)]
pub enum VisibleError<E> {
    List(ListBucketError),
    Shows(E),
    Cursor,
}

/// The token that resumes after `entry`, past its whole group when it is a common prefix.
pub fn after_entry(bucket: &str, entry: &str, group: bool) -> ListContinuationToken {
    ListContinuationToken {
        last_key: format!("{bucket}/{entry}").into_bytes(),
        last_common_prefix: group.then(|| entry.to_string()),
    }
}

enum Probe {
    Shown,
    Empty,
    Stopped(Vec<u8>),
}

async fn list_once<E>(
    context: &DriverContext,
    input: &ListBucketInput,
    bounds: &Option<(String, Option<String>)>,
) -> Result<crate::s3::object::list::ListBucketResult, VisibleError<E>> {
    let operation = match bounds.clone() {
        Some((lower, upper)) => ListBucketOperation::new(input.clone()).with_bounds(lower, upper),
        None => ListBucketOperation::new(input.clone()),
    };
    drive(operation, context).await.map_err(VisibleError::List)
}

/// Seeks one shown entry below `group` from `at`, spending the budget on hidden ones.
async fn probe<'s, E, F>(
    context: &DriverContext,
    (input, bounds): (&ListBucketInput, &Option<(String, Option<String>)>),
    (group, at): (&str, Option<Vec<u8>>),
    budget: &mut usize,
    shows: &mut F,
) -> Result<Probe, VisibleError<E>>
where
    F: FnMut(&ListedObject) -> BoxFuture<'s, Result<bool, E>>,
{
    // The first page holds one entry, as the first entry of a prefix usually shows.
    let mut probe = ListBucketInput {
        prefix: Some(group.to_string()),
        delimiter: None,
        continuation_token: at.map(|last_key| ListContinuationToken {
            last_key,
            last_common_prefix: None,
        }),
        start_after: None,
        max_keys: Some(1),
        ..input.clone()
    };
    loop {
        let list = list_once(context, &probe, bounds).await?;
        for object in list.objects {
            if shows(&object).await.map_err(VisibleError::Shows)? {
                return Ok(Probe::Shown);
            }
            *budget = budget.saturating_sub(1);
            if *budget == 0 {
                let at = object.head.to_bytes().map_err(|_| VisibleError::Cursor)?;
                return Ok(Probe::Stopped(at));
            }
        }
        match list.continuation_token {
            Some(token) => probe.continuation_token = Some(token),
            None => return Ok(Probe::Empty),
        }
        probe.max_keys = None;
    }
}

enum Entry {
    Object(ListedObject),
    Group(String),
}

/// The entries of one listed page in key order.
fn in_order(objects: Vec<ListedObject>, prefixes: Vec<String>) -> Vec<Entry> {
    let mut entries: Vec<Entry> = (objects.into_iter().map(Entry::Object))
        .chain(prefixes.into_iter().map(Entry::Group))
        .collect();
    entries.sort_by(|left, right| name(left).cmp(name(right)));
    entries
}

fn name(entry: &Entry) -> &str {
    match entry {
        Entry::Object(object) => &object.head.key,
        Entry::Group(group) => group,
    }
}

/// One page of the entries `shows` admits, from `input`'s position or a `hidden` one. A common
/// prefix shows once an entry below it does, and `groups` refuses prefixes outright. A page
/// that met hidden entries ends after its last shown one, or sealed when it showed none.
pub async fn visible_page<'s, E, F>(
    context: &DriverContext,
    mut input: ListBucketInput,
    (hidden, bounds): (Option<HiddenCursor>, Option<(String, Option<String>)>),
    mut shows: F,
    groups: impl Fn(&str) -> bool,
) -> Result<VisiblePage, VisibleError<E>>
where
    F: FnMut(&ListedObject) -> BoxFuture<'s, Result<bool, E>>,
{
    let max = (input.max_keys).unwrap_or(ListBucketOperation::DEFAULT_MAX_KEYS);
    let bucket = input.bucket.clone();
    let mut page = VisiblePage::default();
    if max == 0 {
        return Ok(page);
    }
    let mut budget = SCAN_BUDGET;
    let mut last: Option<(String, bool)> = None;
    let sealed = |hidden: &HiddenCursor| {
        seal_cursor(hidden)
            .map(NextPage::Sealed)
            .ok_or(VisibleError::Cursor)
    };
    if let Some(hidden) = hidden {
        input.continuation_token = hidden.position.clone();
        input.start_after = None;
        if let Some((group, at)) = hidden.pending.clone() {
            let probed = probe(
                context,
                (&input, &bounds),
                (&group, at),
                &mut budget,
                &mut shows,
            );
            match probed.await? {
                Probe::Shown => {
                    page.prefixes.push(group.clone());
                    last = Some((group.clone(), true));
                }
                Probe::Empty => {}
                Probe::Stopped(at) => {
                    let pending = Some((group, Some(at)));
                    page.next = Some(sealed(&HiddenCursor { pending, ..hidden })?);
                    return Ok(page);
                }
            }
            input.continuation_token = Some(after_entry(&bucket, &group, true));
        }
    }
    loop {
        let list = list_once(context, &input, &bounds).await?;
        let more = list.continuation_token.clone();
        let entries = in_order(list.objects, list.common_prefixes);
        let count = entries.len();
        for (index, entry) in entries.into_iter().enumerate() {
            let (entry_name, group) = match &entry {
                Entry::Object(object) => (object.head.key.clone(), false),
                Entry::Group(group) => (group.clone(), true),
            };
            let position = Some(after_entry(&bucket, &entry_name, group));
            // A hidden entry costs one; a prefix pays for the hidden entries its probe met.
            let (shown, cost) = match entry {
                Entry::Object(object) => match shows(&object).await.map_err(VisibleError::Shows)? {
                    true => {
                        page.objects.push(object);
                        (true, 0)
                    }
                    false => (false, 1),
                },
                Entry::Group(group) if groups(&group) => {
                    let probed = probe(
                        context,
                        (&input, &bounds),
                        (&group, None),
                        &mut budget,
                        &mut shows,
                    );
                    match probed.await? {
                        Probe::Shown => {
                            page.prefixes.push(group);
                            (true, 0)
                        }
                        Probe::Empty => (false, 0),
                        Probe::Stopped(at) => {
                            page.next = Some(match &last {
                                Some((key, group)) => {
                                    NextPage::After(after_entry(&bucket, key, *group))
                                }
                                None => sealed(&HiddenCursor {
                                    position: input.continuation_token.clone(),
                                    pending: Some((group, Some(at))),
                                })?,
                            });
                            return Ok(page);
                        }
                    }
                }
                Entry::Group(_) => (false, 1),
            };
            budget = budget.saturating_sub(cost);
            if shown {
                last = Some((entry_name, group));
            }
            let rest = index + 1 < count || more.is_some();
            let full = page.objects.len() + page.prefixes.len() >= max;
            if (full || budget == 0) && rest {
                page.next = Some(match &last {
                    Some((key, group)) => NextPage::After(after_entry(&bucket, key, *group)),
                    None => sealed(&HiddenCursor {
                        position,
                        pending: None,
                    })?,
                });
                return Ok(page);
            }
            if !shown {
                input.continuation_token = position;
            }
        }
        match (more, &last) {
            (None, _) => return Ok(page),
            (Some(_), Some((key, group))) => {
                page.next = Some(NextPage::After(after_entry(&bucket, key, *group)));
                return Ok(page);
            }
            (Some(token), None) => {
                input.continuation_token = Some(token);
                input.start_after = None;
            }
        }
    }
}
