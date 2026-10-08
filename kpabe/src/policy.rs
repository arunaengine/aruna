//! Typed attributes and canonical policies with their row labels and limits.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use alloc::vec::Vec;

use crate::{Error, MAX_ATTRIBUTE_BYTES, MAX_ATTRIBUTES, MAX_BYTES, MAX_EPOCHS, MAX_ROWS};

/// Literal, typed facts. Prefix matching is exact; callers supply ancestor prefix attributes.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Attribute {
    Domain(Vec<u8>),
    Epoch(u64),
    Prefix(Vec<u8>),
    Key(Vec<u8>),
    Write([u8; 16]),
}

impl Attribute {
    pub(crate) fn validate(&self) -> Result<(), Error> {
        match self {
            Self::Domain(value) if value.is_empty() => Err(Error),
            Self::Epoch(0) => Err(Error),
            Self::Domain(value) | Self::Prefix(value) | Self::Key(value)
                if value.len() > MAX_ATTRIBUTE_BYTES =>
            {
                Err(Error)
            }
            _ => Ok(()),
        }
    }

    pub(crate) fn encode(&self, output: &mut Vec<u8>) {
        let (tag, value): (u8, &[u8]) = match self {
            Self::Domain(value) => (b'd', value),
            Self::Epoch(value) => (b'e', &value.to_be_bytes()),
            Self::Prefix(value) => (b'p', value),
            Self::Key(value) => (b'k', value),
            Self::Write(value) => (b'w', value),
        };
        output.push(tag);
        output.extend_from_slice(&(value.len() as u32).to_be_bytes());
        output.extend_from_slice(value);
    }
}

/// A domain AND an epoch set, optionally AND a union of literal prefixes, keys or writes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Policy {
    pub(crate) domain: Vec<u8>,
    pub(crate) epochs: Vec<u64>,
    pub(crate) alternatives: Vec<Attribute>,
}

impl Policy {
    /// Rejects missing domain or epochs, repeated labels, invalid alternatives and exceeded limits.
    /// Sorts epoch and alternative sets into their canonical order.
    pub fn new(domain: &[u8], epochs: &[u64], alternatives: &[Attribute]) -> Result<Self, Error> {
        let count = 1usize
            .checked_add(epochs.len())
            .and_then(|count| count.checked_add(alternatives.len()))
            .ok_or(Error)?;
        if domain.is_empty()
            || domain.len() > MAX_ATTRIBUTE_BYTES
            || epochs.is_empty()
            || epochs.len() > MAX_EPOCHS
            || epochs.contains(&0)
            || count > MAX_ROWS
        {
            return Err(Error);
        }
        for alternative in alternatives {
            if !matches!(
                alternative,
                Attribute::Prefix(_) | Attribute::Key(_) | Attribute::Write(_)
            ) {
                return Err(Error);
            }
            alternative.validate()?;
        }
        let mut epochs = epochs.to_vec();
        epochs.sort_unstable();
        let mut alternatives = alternatives.to_vec();
        alternatives.sort_unstable();
        if epochs.windows(2).any(|pair| pair[0] == pair[1])
            || alternatives.windows(2).any(|pair| pair[0] == pair[1])
        {
            return Err(Error);
        }
        let policy = Self {
            domain: domain.to_vec(),
            epochs,
            alternatives,
        };
        canonical_attributes(&policy.labels(), MAX_ROWS)?;
        Ok(policy)
    }

    /// Returns the canonical row labels, with exactly one domain and at least one epoch.
    pub fn labels(&self) -> Vec<Attribute> {
        let mut labels = Vec::with_capacity(1 + self.epochs.len() + self.alternatives.len());
        labels.push(Attribute::Domain(self.domain.clone()));
        labels.extend(self.epochs.iter().copied().map(Attribute::Epoch));
        labels.extend(self.alternatives.iter().cloned());
        labels
    }

    pub(crate) fn matrix(&self) -> Vec<[i8; 3]> {
        let mut matrix = Vec::with_capacity(1 + self.epochs.len() + self.alternatives.len());
        matrix.push([1, 1, 0]);
        matrix.extend(
            self.epochs
                .iter()
                .map(|_| [0, -1, i8::from(!self.alternatives.is_empty())]),
        );
        matrix.extend(self.alternatives.iter().map(|_| [0, 0, -1]));
        matrix
    }

    pub(crate) fn select(&self, attributes: &[Attribute]) -> Result<Vec<(usize, usize)>, Error> {
        let labels = self.labels();
        let find = |index: usize| {
            attributes
                .iter()
                .position(|attribute| attribute == &labels[index])
                .map(|pos| (index, pos))
        };
        let mut selected = Vec::with_capacity(3);
        selected.push(find(0).ok_or(Error)?);
        selected.push((1..=self.epochs.len()).find_map(find).ok_or(Error)?);
        if !self.alternatives.is_empty() {
            selected.push(
                ((1 + self.epochs.len())..labels.len())
                    .find_map(find)
                    .ok_or(Error)?,
            );
        }
        Ok(selected)
    }
}

pub(crate) fn canonical_attributes(
    attributes: &[Attribute],
    limit: usize,
) -> Result<Vec<Attribute>, Error> {
    if attributes.is_empty() || attributes.len() > limit.min(MAX_ATTRIBUTES) {
        return Err(Error);
    }
    let mut size = 0usize;
    for attribute in attributes {
        attribute.validate()?;
        let length = match attribute {
            Attribute::Domain(value) | Attribute::Prefix(value) | Attribute::Key(value) => {
                value.len()
            }
            Attribute::Epoch(_) => 8,
            Attribute::Write(_) => 16,
        };
        size = size.checked_add(5 + length).ok_or(Error)?;
        if size > MAX_BYTES {
            return Err(Error);
        }
    }
    let mut attributes = attributes.to_vec();
    attributes.sort_unstable();
    if attributes.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(Error);
    }
    Ok(attributes)
}
