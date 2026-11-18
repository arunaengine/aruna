//! Lists only what a caller may see: a common prefix shows once an entry below it does, and a
//! cursor names only shown entries, or is sealed when a page stops among hidden ones.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::cursor;
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::link::LinkTarget;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::s3::object::link::{LinkCaller, LinkError, LinkNode, TargetVisibleOperation};
use crate::s3::object::list::{
    ListBucketError, ListBucketInput, ListBucketOperation, ListBucketResult, ListContinuationToken,
    ListedObject,
};
use crate::s3::object::search_scan::{open_position, seal_position, sealed_position};

/// Hidden entries one listing request examines at most. A page that reaches it ends early and
/// its sealed cursor resumes there, so it bounds the work of one request, never its results.
pub const SCAN_BUDGET: usize = 1_000;

/// Starts a sealed cursor; no base64 token of a listing does. A marker key starting with it
/// stays a key unless it has the whole shape of a sealed cursor.
const SEALED_MARK: char = '~';
/// The bytes a sealed position adds to its plain one: its tag, nonce and authentication tag.
const SEALED_OVERHEAD: usize = 1 + 24 + 16;

/// A listing position sealed with this node's key, so it names no entry; a restart of the
/// node makes it invalid.
pub fn seal_cursor(value: &impl Serialize) -> Option<String> {
    let sealed = seal_position(&postcard::to_allocvec(value).ok()?).ok()?;
    Some(format!("{SEALED_MARK}{}", cursor::encode(sealed)))
}

/// Whether `text` has the shape of a sealed cursor: the mark, then base64 of a sealed position.
/// One that does not open, as after a restart of the node, is refused, never read as a key.
pub fn is_sealed(text: &str) -> bool {
    (text.strip_prefix(SEALED_MARK))
        .and_then(|rest| cursor::decode(rest).ok())
        .is_some_and(|sealed| sealed_position(&sealed) && sealed.len() > SEALED_OVERHEAD)
}

/// The position a sealed cursor of this node holds; `None` for any other text.
pub fn open_cursor<T: DeserializeOwned>(text: &str) -> Option<T> {
    let sealed = cursor::decode(text.strip_prefix(SEALED_MARK)?).ok()?;
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
#[derive(Debug, PartialEq)]
pub struct VisiblePage<O = ListedObject> {
    pub objects: Vec<O>,
    pub prefixes: Vec<String>,
    pub next: Option<NextPage>,
}

impl<O> Default for VisiblePage<O> {
    fn default() -> Self {
        Self {
            objects: Vec::new(),
            prefixes: Vec::new(),
            next: None,
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum VisibleError<E, L = ListBucketError> {
    List(L),
    Shows(E),
    Cursor,
    Io(IoError),
}

impl<E, L> From<IoError> for VisibleError<E, L> {
    fn from(error: IoError) -> Self {
        Self::Io(error)
    }
}

/// One page a source answered at a position, in its own key order.
#[derive(Debug, PartialEq)]
pub struct Listed<O> {
    pub objects: Vec<O>,
    pub prefixes: Vec<String>,
    pub next: Option<ListContinuationToken>,
}

/// Where a visible listing reads its pages: this node's records, or another member's.
pub trait ListSource: Send + std::fmt::Debug + PartialEq {
    type Object: Send + std::fmt::Debug + PartialEq + 'static;
    type Error: Send + std::fmt::Debug + PartialEq + 'static;
    fn call(&self, input: &ListBucketInput) -> IoCall;
    fn take(&self, result: IoResult) -> Result<Listed<Self::Object>, ListFailure<Self::Error>>;
    fn key(object: &Self::Object) -> &str;
}

/// Why a source gave no page.
#[derive(Debug, PartialEq)]
pub enum ListFailure<E> {
    Source(E),
    Io(IoError),
}

impl<E> From<IoError> for ListFailure<E> {
    fn from(error: IoError) -> Self {
        Self::Io(error)
    }
}

impl<S, L> From<ListFailure<L>> for VisibleError<S, L> {
    fn from(failure: ListFailure<L>) -> Self {
        match failure {
            ListFailure::Source(error) => Self::List(error),
            ListFailure::Io(error) => Self::Io(error),
        }
    }
}

/// Whether an entry shows: known at once, or after one call.
pub enum Shown {
    Decided(bool),
    Ask(IoCall),
}

/// Which entries of a listing a caller sees.
pub trait ShowRule: Send + std::fmt::Debug + PartialEq {
    type Object;
    type Error: Send + std::fmt::Debug + PartialEq + 'static;
    fn shows(&self, object: &Self::Object) -> Shown;
    /// The answer to the call of [`Shown::Ask`].
    fn answered(&self, result: IoResult) -> Result<Result<bool, Self::Error>, IoError>;
    /// Whether a common prefix may show at all.
    fn groups(&self, prefix: &str) -> bool;
}

/// This node's records of a bucket, inside `bounds` when given.
#[derive(Debug, PartialEq)]
pub struct RecordSource {
    pub bounds: Option<(String, Option<String>)>,
}

impl ListSource for RecordSource {
    type Object = ListedObject;
    type Error = ListBucketError;

    fn call(&self, input: &ListBucketInput) -> IoCall {
        let list = ListBucketOperation::new(input.clone());
        IoCall::child(match self.bounds.clone() {
            Some((lower, upper)) => list.with_bounds(lower, upper),
            None => list,
        })
    }

    fn take(&self, result: IoResult) -> Result<Listed<ListedObject>, ListFailure<ListBucketError>> {
        let listed = result.child::<ListBucketResult, ListBucketError>()?;
        let listed = listed.map_err(ListFailure::Source)?;
        Ok(Listed {
            objects: listed.objects,
            prefixes: listed.common_prefixes,
            next: listed.continuation_token,
        })
    }

    fn key(object: &ListedObject) -> &str {
        &object.head.key
    }
}

/// The keys and common prefixes a caller may see.
pub trait ListingKeys: Send + std::fmt::Debug + PartialEq {
    fn allows(&self, key: &str) -> bool;
    fn groups(&self, prefix: &str) -> bool;
}

/// Shows the allowed keys of a local listing, a link only when its caller may read the target.
#[derive(Debug, PartialEq)]
pub struct LinkShows<K> {
    pub keys: K,
    pub caller: LinkCaller,
    pub realm_id: RealmId,
    pub node: LinkNode,
}

impl<K: ListingKeys> ShowRule for LinkShows<K> {
    type Object = ListedObject;
    type Error = LinkError;

    fn shows(&self, object: &ListedObject) -> Shown {
        if !self.keys.allows(&object.head.key) {
            return Shown::Decided(false);
        }
        let Some(link) = object.link.as_ref() else {
            return Shown::Decided(true);
        };
        match LinkTarget::of(link, self.realm_id) {
            Some(target) => {
                let visible = TargetVisibleOperation::new(&self.caller, &target, self.node);
                Shown::Ask(IoCall::child(visible))
            }
            None => Shown::Decided(false),
        }
    }

    fn answered(&self, result: IoResult) -> Result<Result<bool, LinkError>, IoError> {
        result.child::<bool, LinkError>()
    }

    fn groups(&self, prefix: &str) -> bool {
        self.keys.groups(prefix)
    }
}

/// The token that resumes after `entry`, past its whole group when it is a common prefix.
pub fn after_entry(bucket: &str, entry: &str, group: bool) -> ListContinuationToken {
    ListContinuationToken {
        last_key: format!("{bucket}/{entry}").into_bytes(),
        last_common_prefix: group.then(|| entry.to_string()),
    }
}

#[derive(Debug, PartialEq)]
enum Entry<O> {
    Object(O),
    Group(String),
}

/// An entry being decided: its name, whether it is a group, and the position after it.
#[derive(Debug, PartialEq)]
struct EntryPlace {
    name: String,
    group: bool,
    position: ListContinuationToken,
}

/// What a finished search below a common prefix is for.
#[derive(Debug, PartialEq)]
enum ProbeFor {
    Hidden(HiddenCursor),
    Entry(EntryPlace),
}

/// The search for one shown entry below a common prefix.
#[derive(Debug, PartialEq)]
struct Probe<O> {
    group: String,
    input: ListBucketInput,
    objects: VecDeque<O>,
    next: Option<ListContinuationToken>,
    current: Option<O>,
    purpose: ProbeFor,
}

enum ProbeEnd {
    Shown,
    Empty,
    Stopped(Vec<u8>),
}

#[derive(Debug, PartialEq)]
enum VisibleStep<O> {
    Page,
    Object(O, EntryPlace),
    ProbePage,
    ProbeObject,
}

/// One page of the entries a rule admits, from the input's position or a hidden one. A common
/// prefix shows once an entry below it does. A page that met hidden entries ends after its last
/// shown one, or sealed when it showed none.
#[derive(Debug, PartialEq)]
pub struct VisiblePageOperation<S: ListSource, R> {
    source: S,
    rule: R,
    input: ListBucketInput,
    hidden: Option<HiddenCursor>,
    bucket: String,
    max: usize,
    budget: usize,
    page: VisiblePage<S::Object>,
    last: Option<(String, bool)>,
    entries: VecDeque<Entry<S::Object>>,
    more: Option<ListContinuationToken>,
    probe: Option<Probe<S::Object>>,
    step: VisibleStep<S::Object>,
}

type VisibleTurn<S, R> = Result<
    Next<VisiblePage<<S as ListSource>::Object>>,
    VisibleError<<R as ShowRule>::Error, <S as ListSource>::Error>,
>;

impl<S, R> VisiblePageOperation<S, R>
where
    S: ListSource,
    R: ShowRule<Object = S::Object>,
{
    pub fn new(source: S, rule: R, input: ListBucketInput, hidden: Option<HiddenCursor>) -> Self {
        Self {
            source,
            rule,
            max: (input.max_keys).unwrap_or(ListBucketOperation::DEFAULT_MAX_KEYS),
            bucket: input.bucket.clone(),
            input,
            hidden,
            budget: SCAN_BUDGET,
            page: VisiblePage::default(),
            last: None,
            entries: VecDeque::new(),
            more: None,
            probe: None,
            step: VisibleStep::Page,
        }
    }

    fn done(&mut self) -> VisibleTurn<S, R> {
        Ok(Next::Done(std::mem::take(&mut self.page)))
    }

    fn end_at(&mut self, next: NextPage) -> VisibleTurn<S, R> {
        self.page.next = Some(next);
        self.done()
    }

    fn sealed(&self, hidden: &HiddenCursor) -> Result<NextPage, VisibleError<R::Error, S::Error>> {
        seal_cursor(hidden)
            .map(NextPage::Sealed)
            .ok_or(VisibleError::Cursor)
    }

    fn after_last(&self) -> Option<NextPage> {
        let (key, group) = self.last.as_ref()?;
        Some(NextPage::After(after_entry(&self.bucket, key, *group)))
    }

    fn list_page(&mut self) -> VisibleTurn<S, R> {
        self.step = VisibleStep::Page;
        Ok(Next::Call(self.source.call(&self.input)))
    }

    fn page_read(&mut self, list: Listed<S::Object>) -> VisibleTurn<S, R> {
        self.more = list.next;
        let mut entries: Vec<_> = (list.objects.into_iter().map(Entry::Object))
            .chain(list.prefixes.into_iter().map(Entry::Group))
            .collect();
        entries.sort_by(|left, right| Self::name(left).cmp(Self::name(right)));
        self.entries = entries.into();
        self.next_entry()
    }

    fn name(entry: &Entry<S::Object>) -> &str {
        match entry {
            Entry::Object(object) => S::key(object),
            Entry::Group(group) => group,
        }
    }

    fn next_entry(&mut self) -> VisibleTurn<S, R> {
        let Some(entry) = self.entries.pop_front() else {
            return match (self.more.take(), self.after_last()) {
                (None, _) => self.done(),
                (Some(_), Some(next)) => self.end_at(next),
                (Some(token), None) => {
                    self.input.continuation_token = Some(token);
                    self.input.start_after = None;
                    self.list_page()
                }
            };
        };
        let name = Self::name(&entry).to_string();
        let group = matches!(entry, Entry::Group(_));
        let position = after_entry(&self.bucket, &name, group);
        let place = EntryPlace {
            name,
            group,
            position,
        };
        match entry {
            Entry::Object(object) => match self.rule.shows(&object) {
                Shown::Decided(shown) => self.object_decided(object, place, shown),
                Shown::Ask(call) => {
                    self.step = VisibleStep::Object(object, place);
                    Ok(Next::Call(call))
                }
            },
            Entry::Group(prefix) if self.rule.groups(&prefix) => {
                self.start_probe(prefix, None, ProbeFor::Entry(place))
            }
            Entry::Group(_) => self.entry_done(place, false, 1),
        }
    }

    fn object_decided(
        &mut self,
        object: S::Object,
        place: EntryPlace,
        shown: bool,
    ) -> VisibleTurn<S, R> {
        if !shown {
            return self.entry_done(place, false, 1);
        }
        self.page.objects.push(object);
        self.entry_done(place, true, 0)
    }

    /// A hidden entry costs one; a prefix pays for the hidden entries its probe met.
    fn entry_done(&mut self, place: EntryPlace, shown: bool, cost: usize) -> VisibleTurn<S, R> {
        let EntryPlace {
            name,
            group,
            position,
        } = place;
        self.budget = self.budget.saturating_sub(cost);
        if shown {
            self.last = Some((name, group));
        }
        let rest = !self.entries.is_empty() || self.more.is_some();
        let full = self.page.objects.len() + self.page.prefixes.len() >= self.max;
        if (full || self.budget == 0) && rest {
            let next = match self.after_last() {
                Some(next) => next,
                None => self.sealed(&HiddenCursor {
                    position: Some(position),
                    pending: None,
                })?,
            };
            return self.end_at(next);
        }
        if !shown {
            self.input.continuation_token = Some(position);
        }
        self.next_entry()
    }

    /// Seeks one shown entry below `group`; the first page holds one entry, as the first entry
    /// of a prefix usually shows.
    fn start_probe(
        &mut self,
        group: String,
        at: Option<Vec<u8>>,
        purpose: ProbeFor,
    ) -> VisibleTurn<S, R> {
        let input = ListBucketInput {
            prefix: Some(group.clone()),
            delimiter: None,
            continuation_token: at.map(|last_key| ListContinuationToken {
                last_key,
                last_common_prefix: None,
            }),
            start_after: None,
            max_keys: Some(1),
            ..self.input.clone()
        };
        let call = self.source.call(&input);
        self.probe = Some(Probe {
            group,
            input,
            objects: VecDeque::new(),
            next: None,
            current: None,
            purpose,
        });
        self.step = VisibleStep::ProbePage;
        Ok(Next::Call(call))
    }

    fn probe_next(&mut self) -> VisibleTurn<S, R> {
        let probe = self.probe.as_mut().ok_or(IoError::Unfinished)?;
        let Some(object) = probe.objects.pop_front() else {
            let Some(token) = probe.next.take() else {
                return self.probe_end(ProbeEnd::Empty);
            };
            probe.input.continuation_token = Some(token);
            probe.input.max_keys = None;
            let call = self.source.call(&probe.input);
            self.step = VisibleStep::ProbePage;
            return Ok(Next::Call(call));
        };
        match self.rule.shows(&object) {
            Shown::Decided(shown) => self.probe_decided(object, shown),
            Shown::Ask(call) => {
                probe.current = Some(object);
                self.step = VisibleStep::ProbeObject;
                Ok(Next::Call(call))
            }
        }
    }

    fn probe_decided(&mut self, object: S::Object, shown: bool) -> VisibleTurn<S, R> {
        if shown {
            return self.probe_end(ProbeEnd::Shown);
        }
        probe.max_keys = None;
    }
}

enum Entry<O> {
    Object(O),
    Group(String),
}

/// The entries of one listed page in key order.
fn in_order<L: Lister>(objects: Vec<L::Object>, prefixes: Vec<String>) -> Vec<Entry<L::Object>> {
    let mut entries: Vec<Entry<L::Object>> = (objects.into_iter().map(Entry::Object))
        .chain(prefixes.into_iter().map(Entry::Group))
        .collect();
    entries.sort_by(|left, right| name::<L>(left).cmp(name::<L>(right)));
    entries
}

fn name<L: Lister>(entry: &Entry<L::Object>) -> &str {
    match entry {
        Entry::Object(object) => L::key(object),
        Entry::Group(group) => group,
    }
}

/// One page of the entries `shows` admits, from `input`'s position or a `hidden` one. A common
/// prefix shows once an entry below it does, and `groups` refuses prefixes outright. A page
/// that met hidden entries ends after its last shown one, or sealed when it showed none.
pub async fn visible_page<'s, E, F>(
    context: &DriverContext,
    input: ListBucketInput,
    (hidden, bounds): (Option<HiddenCursor>, Option<(String, Option<String>)>),
    shows: F,
    groups: impl Fn(&str) -> bool,
) -> Result<VisiblePage, VisibleError<E>>
where
    F: FnMut(&ListedObject) -> BoxFuture<'s, Result<bool, E>>,
{
    let records = Records { context, bounds };
    visible_from(&records, input, hidden, shows, groups).await
}

/// `visible_page` over the pages of `lister`.
pub async fn visible_from<'s, L: Lister, E, F>(
    lister: &L,
    mut input: ListBucketInput,
    hidden: Option<HiddenCursor>,
    mut shows: F,
    groups: impl Fn(&str) -> bool,
) -> Result<VisiblePage<L::Object>, VisibleError<E, L::Error>>
where
    F: FnMut(&L::Object) -> BoxFuture<'s, Result<bool, E>>,
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
            let probed = probe(lister, &input, (&group, at), &mut budget, &mut shows);
            match probed.await? {
                Probe::Shown => {
                    page.prefixes.push(group.clone());
                    last = Some((group.clone(), true));
                    // A page the prefix fills ends here, and the next one starts past it.
                    if page.prefixes.len() >= max {
                        page.next = Some(NextPage::After(after_entry(&bucket, &group, true)));
                        return Ok(page);
                    }
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
        let list = lister.list(&input).await.map_err(VisibleError::List)?;
        let more = list.next.clone();
        let entries = in_order::<L>(list.objects, list.prefixes);
        let count = entries.len();
        for (index, entry) in entries.into_iter().enumerate() {
            let (entry_name, group) = match &entry {
                Entry::Object(object) => (L::key(object).to_string(), false),
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
                    let probed = probe(lister, &input, (&group, None), &mut budget, &mut shows);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tells_sealed_markers() {
        // A sealed cursor opens here; one sealed elsewhere keeps its shape but does not open,
        // and a key that merely starts with the mark is no sealed cursor.
        let cursor = seal_cursor(&HiddenCursor::default()).unwrap();
        assert!(is_sealed(&cursor));
        assert_eq!(open_cursor(&cursor), Some(HiddenCursor::default()));
        let foreign = format!("~{}", cursor::encode([0xFF; 64]));
        assert!(is_sealed(&foreign));
        assert_eq!(open_cursor::<HiddenCursor>(&foreign), None);
        assert!(!is_sealed("~notes.txt"));
        assert!(!is_sealed("plain"));
    }
}
