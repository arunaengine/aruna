use obkv::{KvIter, KvReaderU16};
use std::iter::{Fuse, Peekable};
use thiserror::Error;

#[derive(Error, Debug)]
#[error("Failed to parse field: {0}")]
pub struct ParseError(pub String);

// Helper struct to wrap the KvReader iterator
pub struct FieldIterator<'a> {
    iter: Peekable<Fuse<KvIter<'a, u16>>>,
}

impl<'a> FieldIterator<'a> {
    pub fn new(reader: &'a KvReaderU16) -> Self {
        Self {
            iter: reader.iter().peekable(),
        }
    }

    /// Get the next field if it matches the expected index
    /// otherwise return the default value of the type
    /// Use get_required_field if the field is required
    pub fn get_field<T: serde::de::DeserializeOwned + Default>(
        &mut self,
        expected_index: u16,
    ) -> Result<T, ParseError> {
        match self.iter.peek() {
            Some(&(index, _)) if index == expected_index => {
                if let Some((_, value)) = self.iter.next() {
                    serde_json::from_slice(value).map_err(|e| {
                        ParseError(format!(
                            "Failed to deserialize field {}: {}",
                            expected_index, e
                        ))
                    })
                } else {
                    unreachable!("This was checked by peek() before")
                }
            }
            _ => Ok(T::default()),
        }
    }

    /// Get the next field if it matches the expected index
    /// otherwise return a parse error
    pub fn get_required_field<T: serde::de::DeserializeOwned>(
        &mut self,
        expected_index: u16,
    ) -> Result<T, ParseError> {
        match self.iter.next() {
            Some((index, value)) if index == expected_index => serde_json::from_slice(value)
                .map_err(|e| {
                    ParseError(format!(
                        "Failed to deserialize required field {}: {}",
                        expected_index, e
                    ))
                }),
            _ => Err(ParseError(format!(
                "Missing required field {}",
                expected_index
            ))),
        }
    }
}
