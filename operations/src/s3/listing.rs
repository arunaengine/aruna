use std::cmp::Ordering;
use ulid::Ulid;

/// Compute the S3 common prefix for `key` under the given `prefix`/`delimiter`,
/// or `None` when the key has no delimiter past the prefix (so it is listed as
/// an individual entry). Shared by the delimiter-grouping list operations.
pub fn common_prefix_of(
    key: &str,
    prefix: Option<&str>,
    delimiter: Option<&str>,
) -> Option<String> {
    let delimiter = delimiter.filter(|delimiter| !delimiter.is_empty())?;
    let prefix_len = prefix.map_or(0, str::len);
    let relative_match = key.get(prefix_len..)?.find(delimiter)?;
    Some(key[..prefix_len + relative_match + delimiter.len()].to_string())
}

/// Resume point of a keyed listing: entries up to and including the marker are
/// skipped, and `id` only orders entries sharing the marker key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ListMarker<'a> {
    pub(crate) key: &'a str,
    pub(crate) id: Option<Ulid>,
}

impl ListMarker<'_> {
    /// Whether `key`/`id` sorts strictly after this marker.
    pub(crate) fn is_after(&self, key: &str, id: Ulid) -> bool {
        match key.cmp(self.key) {
            Ordering::Less => false,
            Ordering::Greater => true,
            Ordering::Equal => self.id.is_some_and(|marker| id > marker),
        }
    }
}

/// Drops entries at or before `marker`; `key_of`/`id_of` supply the sort pair.
pub(crate) fn retain_after_marker<T>(
    entries: &mut Vec<T>,
    marker: Option<ListMarker<'_>>,
    key_of: impl Fn(&T) -> &str,
    id_of: impl Fn(&T) -> Ulid,
) {
    let Some(marker) = marker else {
        return;
    };
    entries.retain(|entry| marker.is_after(key_of(entry), id_of(entry)));
}

/// Common prefixes emitted by one page, the group a resume marker already
/// represents, and the most recently emitted group.
#[derive(Debug, Default, PartialEq)]
pub(crate) struct PrefixTracker {
    prefixes: Vec<String>,
    resume: Option<String>,
    last: Option<String>,
}

impl PrefixTracker {
    pub(crate) fn count(&self) -> usize {
        self.prefixes.len()
    }

    pub(crate) fn take(&mut self) -> Vec<String> {
        std::mem::take(&mut self.prefixes)
    }

    pub(crate) fn set_resume(&mut self, group: Option<String>) {
        self.resume = group;
    }

    pub(crate) fn resume(&self) -> Option<&str> {
        self.resume.as_deref()
    }

    /// Whether `group` is already represented by the resume marker or the last
    /// emitted group, so its remaining keys can be skipped.
    pub(crate) fn already_emitted(&self, group: &str) -> bool {
        self.resume.as_deref() == Some(group) || self.last.as_deref() == Some(group)
    }

    pub(crate) fn push(&mut self, group: String) {
        self.last = Some(group.clone());
        self.prefixes.push(group);
    }

    /// Forgets the trailing group so a later key in it is emitted again.
    pub(crate) fn clear_last(&mut self) {
        self.last = None;
    }
}

/// Returns the entries after the first one whose id equals `marker`; an absent
/// marker keeps the list and a marker absent from it yields none.
pub(crate) fn split_after_marker<T>(
    mut entries: Vec<T>,
    marker: Option<Ulid>,
    id_of: impl Fn(&T) -> Ulid,
) -> Vec<T> {
    let Some(marker) = marker else {
        return entries;
    };
    match entries.iter().position(|entry| id_of(entry) == marker) {
        Some(index) => entries.split_off(index + 1),
        None => Vec::new(),
    }
}

/// One listing page: retained entries, emitted prefixes, the last
/// emitted index and whether input entries remained.
#[derive(Debug)]
pub(crate) struct ListingPage<T> {
    pub(crate) entries: Vec<T>,
    pub(crate) prefixes: Vec<String>,
    pub(crate) truncated: bool,
    pub(crate) last_index: Option<usize>,
}

/// Builds one listing page from sorted entries: consecutive entries sharing a
/// common prefix collapse into one prefix, other entries are cloned, and `limit`
/// caps the number of emitted entries.
pub(crate) fn build_page<T: Clone>(
    entries: &[T],
    limit: usize,
    prefix: Option<&str>,
    delimiter: Option<&str>,
    key_of: impl Fn(&T) -> &str,
) -> ListingPage<T> {
    let mut kept = Vec::new();
    let mut prefixes = Vec::new();
    let mut last_index = None;
    let mut index = 0;

    while index < entries.len() {
        if kept.len() + prefixes.len() >= limit {
            return ListingPage {
                entries: kept,
                prefixes,
                truncated: true,
                last_index,
            };
        }
        match common_prefix_of(key_of(&entries[index]), prefix, delimiter) {
            Some(group) => {
                let mut last = index;
                while last + 1 < entries.len()
                    && common_prefix_of(key_of(&entries[last + 1]), prefix, delimiter).as_deref()
                        == Some(group.as_str())
                {
                    last += 1;
                }
                last_index = Some(last);
                prefixes.push(group);
                index = last + 1;
            }
            None => {
                last_index = Some(index);
                kept.push(entries[index].clone());
                index += 1;
            }
        }
    }

    ListingPage {
        entries: kept,
        prefixes,
        truncated: false,
        last_index,
    }
}
