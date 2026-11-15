//! The manual form: the values of the `[manual]` fields a person gave, defaults filled in.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde::de::DeserializeOwned;
use serde_json::{Map, Value};

use crate::objects::{self, Object};
use crate::{Error, Result};

#[derive(Debug, Clone, Default, PartialEq)]
pub struct Form {
    fields: Map<String, Value>,
}

impl Form {
    pub(crate) fn parse(text: &str) -> Result<Self> {
        if text.trim().is_empty() {
            return Ok(Self::default());
        }
        match serde_json::from_str(text) {
            Ok(Value::Object(fields)) => Ok(Self { fields }),
            _ => Err(Error::InvalidInput("the form is not a JSON object".into())),
        }
    }

    /// The field `name` as a typed value.
    pub fn get<T: DeserializeOwned>(&self, name: &str) -> Result<T> {
        let value = self.value(name).ok_or_else(|| missing(name))?;
        serde_json::from_value(value.clone())
            .map_err(|error| Error::InvalidInput(format!("form field `{name}`: {error}")))
    }

    pub fn text(&self, name: &str) -> Result<String> {
        self.get(name)
    }

    /// The object a field of type `object` names, loaded.
    pub fn object(&self, name: &str) -> Result<Object> {
        objects::get(&self.text(name)?)
    }

    pub fn value(&self, name: &str) -> Option<&Value> {
        self.fields.get(name)
    }

    pub fn fields(&self) -> &Map<String, Value> {
        &self.fields
    }
}

fn missing(name: &str) -> Error {
    Error::InvalidInput(format!("the form has no field `{name}`"))
}
