//! Checks a declared outgoing connection: its https address template, methods and response fields.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use url::Url;

pub const MAX_URL: usize = 512;
pub const MAX_FIELDS: usize = 32;
pub const MAX_POINTER: usize = 256;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum Method {
    Get,
    Head,
    Post,
    Put,
    Patch,
    Delete,
}

impl Method {
    /// Whether a request may change remote state, so the host records it before sending.
    pub fn mutating(self) -> bool {
        !matches!(self, Self::Get | Self::Head)
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Get => "GET",
            Self::Head => "HEAD",
            Self::Post => "POST",
            Self::Put => "PUT",
            Self::Patch => "PATCH",
            Self::Delete => "DELETE",
        }
    }
}

fn get_only() -> BTreeSet<Method> {
    BTreeSet::from([Method::Get])
}

/// One destination a plugin may send to. Each becomes its own `connections.send` grant.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Connection {
    /// `https://host/prefix`, or `{setting}/prefix` where the setting holds an https origin.
    pub url: String,
    #[serde(default = "get_only")]
    pub methods: BTreeSet<Method>,
    /// Secret slot the host injects into every request.
    #[serde(default)]
    pub auth: Option<String>,
    /// JSON pointers of the answer the code may read; empty means status only.
    #[serde(default)]
    pub response: Vec<String>,
    #[serde(default)]
    pub optional: bool,
}

/// The parts of an address template.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Address<'a> {
    /// A fixed https origin and path prefix.
    Fixed(Url),
    /// The origin comes from the named setting; the path prefix is fixed.
    Setting { setting: &'a str, prefix: &'a str },
}

impl Connection {
    /// Splits the address template, or says why it could reach another host.
    pub fn address(&self) -> Result<Address<'_>, &'static str> {
        if self.url.len() > MAX_URL {
            return Err("the address is too long");
        }
        if let Some(rest) = self.url.strip_prefix('{') {
            let (setting, prefix) = rest.split_once('}').ok_or("an unclosed setting name")?;
            if !valid_prefix(prefix) {
                return Err("the path after the setting is not a plain path prefix");
            }
            return Ok(Address::Setting { setting, prefix });
        }
        let url = Url::parse(&self.url).map_err(|_| "not an absolute address")?;
        let plain = url.scheme() == "https"
            && url.host_str().is_some()
            && url.username().is_empty()
            && url.password().is_none()
            && url.query().is_none()
            && url.fragment().is_none();
        if !plain {
            return Err("only https addresses without credentials, query or fragment");
        }
        // The parser also accepts forms like `https:host`; only the plain form is kept.
        let rest =
            (self.url.strip_prefix("https://")).ok_or("the address must start with https://")?;
        let path = rest.find('/').map_or("", |start| &rest[start..]);
        if self.url.contains('{') || !valid_prefix(path) {
            return Err("the path is not a plain path prefix");
        }
        Ok(Address::Fixed(url))
    }

    /// The base address with its setting filled in: an https origin and the fixed path prefix.
    /// A setting gives the origin only; its path is ignored.
    pub fn base(&self, settings: &serde_json::Value) -> Result<Url, &'static str> {
        let (setting, prefix) = match self.address()? {
            Address::Fixed(url) => return Ok(url),
            Address::Setting { setting, prefix } => (setting, prefix),
        };
        let origin = (settings.get(setting).and_then(serde_json::Value::as_str))
            .ok_or("the address setting has no value")?;
        let origin = Url::parse(origin).map_err(|_| "the address setting is not an address")?;
        if origin.scheme() != "https" || origin.host_str().is_none() {
            return Err("the address setting is not an https origin");
        }
        let joined = format!("{}{prefix}", origin.origin().ascii_serialization());
        Url::parse(&joined).map_err(|_| "the address does not parse")
    }

    /// Checks methods and response fields; the address is checked by [`Connection::address`].
    pub fn check_fields(&self) -> Result<(), &'static str> {
        if self.methods.is_empty() {
            return Err("no method");
        }
        if self.response.len() > MAX_FIELDS {
            return Err("too many response fields");
        }
        let pointer = |field: &String| {
            field.starts_with('/') && field.len() <= MAX_POINTER && !field.contains('\0')
        };
        if !self.response.iter().all(pointer) {
            return Err("a response field is not a JSON pointer");
        }
        Ok(())
    }
}

/// Whether a request path below a connection's prefix is empty or plain segments, so it can
/// never leave the prefix.
pub fn relative_path(path: &str) -> bool {
    path.is_empty() || (!path.starts_with('/') && valid_prefix(&format!("/{path}")))
}

/// Whether `path` is empty or `/`-separated plain segments that cannot leave the prefix.
fn valid_prefix(path: &str) -> bool {
    if path.is_empty() {
        return true;
    }
    let plain = |byte: u8| byte.is_ascii_alphanumeric() || b"/-._~".contains(&byte);
    path.starts_with('/')
        && path.bytes().all(plain)
        && path
            .split('/')
            .all(|segment| segment != "." && segment != "..")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn connection(url: &str) -> Connection {
        Connection {
            url: url.to_string(),
            methods: get_only(),
            auth: None,
            response: Vec::new(),
            optional: false,
        }
    }

    #[test]
    fn address_forms() {
        let fixed = connection("https://catalog.example.org/api/records");
        assert!(matches!(fixed.address(), Ok(Address::Fixed(url)) if url.path() == "/api/records"));
        let setting = connection("{catalog_url}/api/records");
        assert_eq!(
            setting.address(),
            Ok(Address::Setting {
                setting: "catalog_url",
                prefix: "/api/records"
            })
        );
        assert!(connection("https://catalog.example.org").address().is_ok());
        let international = connection("https://ä.example/api");
        let international = international.address();
        assert!(
            matches!(international, Ok(Address::Fixed(url)) if url.host_str() == Some("xn--4ca.example"))
        );
    }

    #[test]
    fn rejects_escapes() {
        for url in [
            "http://catalog.example.org/api",
            "https://user:pw@catalog.example.org/api",
            "https://catalog.example.org/api?x=1",
            "https://catalog.example.org/api#x",
            "https://catalog.example.org/../api",
            "https://catalog.example.org/api/{id}",
            "{catalog_url}@evil.example/api",
            "{catalog_url}.evil.example/api",
            "{catalog_url}/api/%2e%2e",
            "{catalog_url",
            "/api/records",
            "https:x",
            "https:/x",
            "https:",
            "h",
            "https://catalog.example.org/äpi",
            "{catalog_url}/äpi",
        ] {
            assert!(connection(url).address().is_err(), "{url}");
        }
    }

    #[test]
    fn checks_fields() {
        let mut item = connection("https://catalog.example.org");
        assert!(item.check_fields().is_ok());
        item.response = vec!["/id".into(), "/meta/name".into()];
        assert!(item.check_fields().is_ok());
        item.response.push("id".into());
        assert!(item.check_fields().is_err());
        item.response.clear();
        item.methods.clear();
        assert!(item.check_fields().is_err());
        assert!(Method::Post.mutating() && !Method::Head.mutating());
    }
}
