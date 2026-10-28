//! Input pages: the inputs of a stored spec or launch that do not fit its record, kept as a
//! chain of immutable pages that the signed record binds by the first page's digest.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::scheduling::PlannedInput;

/// Encoded input bytes a record keeps inline; larger input lists move into pages.
pub const INLINE_INPUT_BYTES: usize = 256 * 1024;
/// Encoded entry bytes one page holds at most, well inside one record frame.
pub const PAGE_INPUT_BYTES: usize = 512 * 1024;

const PAGE_DIGEST_DOMAIN: &[u8] = b"aruna-input-page-v1";

/// What a record says about its paged inputs: how many, in how many pages, starting where.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PageManifest {
    pub count: u64,
    pub pages: u32,
    pub first: [u8; 32],
}

/// The entries of one page: a spec's selections with their captures, or a launch's pins.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PageEntries {
    Spec(Vec<(InputSelection, CapturedInput)>),
    Launch(Vec<PlannedInput>),
}

impl PageEntries {
    fn len(&self) -> usize {
        match self {
            Self::Spec(entries) => entries.len(),
            Self::Launch(entries) => entries.len(),
        }
    }
}

/// One immutable page and the digest of the page after it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InputPage {
    pub index: u32,
    pub entries: PageEntries,
    pub next: Option<[u8; 32]>,
}

#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum PageError {
    #[error("input page {index} does not match its digest")]
    Digest { index: u32 },
    #[error("input page chain does not match its manifest")]
    Chain,
    #[error("input page kind does not match its record")]
    Kind,
    #[error("input page encoding failed: {0}")]
    Encoding(String),
}

impl From<ConversionError> for PageError {
    fn from(error: ConversionError) -> Self {
        Self::Encoding(error.to_string())
    }
}

impl InputPage {
    pub fn digest(&self) -> Result<[u8; 32], ConversionError> {
        let mut hasher = blake3::Hasher::new();
        hasher.update(PAGE_DIGEST_DOMAIN);
        hasher.update(&postcard::to_allocvec(self)?);
        Ok(*hasher.finalize().as_bytes())
    }
}

/// Whether `entries` would push a record past its inline budget.
pub fn needs_pages<T: Serialize>(entries: &T) -> Result<bool, ConversionError> {
    Ok(postcard::to_allocvec(entries)?.len() > INLINE_INPUT_BYTES)
}

/// Splits entries into pages of at most `PAGE_INPUT_BYTES` each (one oversized entry gets a
/// page of its own) and chains them back to front, so each page names the next one.
pub fn paginate<T: Serialize + Clone>(
    entries: &[T],
    wrap: fn(Vec<T>) -> PageEntries,
) -> Result<(PageManifest, Vec<InputPage>), ConversionError> {
    let mut groups: Vec<Vec<T>> = Vec::new();
    let mut bytes = 0usize;
    for entry in entries {
        let size = postcard::to_allocvec(entry)?.len();
        match groups.last_mut() {
            Some(group) if bytes + size <= PAGE_INPUT_BYTES => group.push(entry.clone()),
            _ => {
                groups.push(vec![entry.clone()]);
                bytes = 0;
            }
        }
        bytes += size;
    }
    let mut pages = Vec::with_capacity(groups.len());
    let mut next = None;
    for (index, group) in groups.into_iter().enumerate().rev() {
        let page = InputPage {
            index: u32::try_from(index).map_err(|_| {
                ConversionError::InvalidLength("input pages exceed the page index".to_string())
            })?,
            entries: wrap(group),
            next,
        };
        next = Some(page.digest()?);
        pages.push(page);
    }
    pages.reverse();
    let manifest = PageManifest {
        count: entries.len() as u64,
        pages: pages.len() as u32,
        first: next.unwrap_or([0; 32]),
    };
    Ok((manifest, pages))
}

/// Checks a full chain against its manifest: every digest, order, page count and entry count.
pub fn verify_chain(manifest: &PageManifest, pages: &[InputPage]) -> Result<(), PageError> {
    let mut expected = (manifest.pages > 0).then_some(manifest.first);
    let mut count = 0u64;
    if pages.len() != manifest.pages as usize {
        return Err(PageError::Chain);
    }
    for (index, page) in pages.iter().enumerate() {
        if page.index as usize != index || expected != Some(page.digest()?) {
            return Err(PageError::Digest { index: page.index });
        }
        count += page.entries.len() as u64;
        expected = page.next;
    }
    if expected.is_some() || count != manifest.count {
        return Err(PageError::Chain);
    }
    Ok(())
}

/// The spec entries of a verified chain, in order.
pub fn spec_entries(
    pages: Vec<InputPage>,
) -> Result<Vec<(InputSelection, CapturedInput)>, PageError> {
    let mut entries = Vec::new();
    for page in pages {
        match page.entries {
            PageEntries::Spec(page) => entries.extend(page),
            PageEntries::Launch(_) => return Err(PageError::Kind),
        }
    }
    Ok(entries)
}

/// The launch pins of a verified chain, in order.
pub fn launch_entries(pages: Vec<InputPage>) -> Result<Vec<PlannedInput>, PageError> {
    let mut entries = Vec::new();
    for page in pages {
        match page.entries {
            PageEntries::Launch(page) => entries.extend(page),
            PageEntries::Spec(_) => return Err(PageError::Kind),
        }
    }
    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pin(index: u32, padding: usize) -> PlannedInput {
        PlannedInput {
            destination_key: format!("{index:08}{}", "x".repeat(padding)),
            version_id: Ulid::from_parts(u64::from(index), 1),
            content: InputContent::Blake3([index as u8; 32]),
            bytes: 1,
            policies: Vec::new(),
            source_node_id: None,
            transfer_ms: 0,
            known_link: false,
        }
    }

    #[test]
    fn chain_round_trip() {
        // Many large entries span several pages, and the verified chain gives them back in order.
        let pins: Vec<_> = (0..40).map(|index| pin(index, 40 * 1024)).collect();
        assert!(needs_pages(&pins).unwrap());
        let (manifest, pages) = paginate(&pins, PageEntries::Launch).unwrap();
        assert!(manifest.pages > 1);
        assert_eq!(manifest.count, 40);
        verify_chain(&manifest, &pages).unwrap();
        assert_eq!(launch_entries(pages).unwrap(), pins);
    }

    #[test]
    fn refuses_changed_chain() {
        // A changed, dropped or swapped page breaks the chain the record binds.
        let pins: Vec<_> = (0..40).map(|index| pin(index, 40 * 1024)).collect();
        let (manifest, pages) = paginate(&pins, PageEntries::Launch).unwrap();
        let mut changed = pages.clone();
        if let PageEntries::Launch(entries) = &mut changed[1].entries {
            entries[0].bytes = 2;
        }
        assert!(verify_chain(&manifest, &changed).is_err());
        assert!(verify_chain(&manifest, &pages[..pages.len() - 1]).is_err());
        let mut swapped = pages.clone();
        swapped.swap(0, 1);
        assert!(verify_chain(&manifest, &swapped).is_err());
        let fewer = PageManifest {
            count: 39,
            ..manifest
        };
        assert!(verify_chain(&fewer, &pages).is_err());
        assert_eq!(spec_entries(pages), Err(PageError::Kind));
    }
}
