//! Converts the values of host calls between the node's types and the WIT types the guest sees.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_plugin_contract::calls;

use crate::host::wit::aruna::plugin::{
    jobs, metadata_create, metadata_read, objects_read, services, state, types,
};

impl From<types::Field> for calls::Field {
    fn from(field: types::Field) -> Self {
        Self {
            name: field.name,
            value: field.value,
        }
    }
}

impl From<calls::Field> for types::Field {
    fn from(field: calls::Field) -> Self {
        Self {
            name: field.name,
            value: field.value,
        }
    }
}

/// The guest's fields as the node handles them.
pub(crate) fn node_fields(fields: Vec<types::Field>) -> Vec<calls::Field> {
    fields.into_iter().map(calls::Field::from).collect()
}

/// The fields as the guest reads them.
pub(crate) fn guest_fields(fields: Vec<calls::Field>) -> Vec<types::Field> {
    fields.into_iter().map(types::Field::from).collect()
}

impl From<services::Method> for calls::Method {
    fn from(method: services::Method) -> Self {
        match method {
            services::Method::Get => Self::Get,
            services::Method::Head => Self::Head,
            services::Method::Post => Self::Post,
            services::Method::Put => Self::Put,
            services::Method::Patch => Self::Patch,
            services::Method::Delete => Self::Delete,
        }
    }
}

impl From<calls::ErrorKind> for types::ErrorKind {
    fn from(kind: calls::ErrorKind) -> Self {
        match kind {
            calls::ErrorKind::NotGranted => Self::NotGranted,
            calls::ErrorKind::Denied => Self::Denied,
            calls::ErrorKind::Invalid => Self::Invalid,
            calls::ErrorKind::Limit => Self::Limit,
            calls::ErrorKind::Conflict => Self::Conflict,
            calls::ErrorKind::UnknownOutcome => Self::UnknownOutcome,
            calls::ErrorKind::Unavailable => Self::Unavailable,
            calls::ErrorKind::NotFound => Self::NotFound,
        }
    }
}

impl From<calls::HostError> for types::HostError {
    fn from(error: calls::HostError) -> Self {
        Self {
            kind: error.kind.into(),
            message: error.message,
        }
    }
}

impl From<calls::Document> for metadata_read::Document {
    fn from(document: calls::Document) -> Self {
        Self {
            resource: document.resource,
            revision: document.revision,
            json: document.json,
        }
    }
}

impl From<calls::Listing> for metadata_read::Listing {
    fn from(listing: calls::Listing) -> Self {
        Self {
            resources: listing.resources,
            cursor: listing.cursor,
        }
    }
}

impl From<calls::FilePage> for metadata_read::FilePage {
    fn from(page: calls::FilePage) -> Self {
        Self {
            files: page.files,
            cursor: page.cursor,
        }
    }
}

impl From<calls::Created> for metadata_create::Created {
    fn from(created: calls::Created) -> Self {
        Self {
            resource: created.resource,
            revision: created.revision,
        }
    }
}

impl From<calls::ObjectInfo> for objects_read::ObjectInfo {
    fn from(info: calls::ObjectInfo) -> Self {
        Self {
            path: info.path,
            size: info.size,
            version: info.version,
            marked: info.marked,
        }
    }
}

impl From<calls::ObjectPage> for objects_read::ObjectPage {
    fn from(page: calls::ObjectPage) -> Self {
        Self {
            objects: page.objects.into_iter().map(Into::into).collect(),
            cursor: page.cursor,
        }
    }
}

impl From<calls::Response> for services::Response {
    fn from(response: calls::Response) -> Self {
        Self {
            status: response.status,
            headers: guest_fields(response.headers),
            body: response.body,
        }
    }
}

impl From<calls::Entry> for state::Entry {
    fn from(entry: calls::Entry) -> Self {
        Self {
            version: entry.version,
            value: entry.value,
        }
    }
}

impl From<calls::JobStatus> for jobs::Status {
    fn from(status: calls::JobStatus) -> Self {
        Self {
            id: status.id,
            kind: status.kind,
            path: status.path,
            state: status.state,
            message: status.message,
            bytes: status.bytes,
            version: status.version,
            reply_status: status.reply_status,
            reply_headers: guest_fields(status.reply_headers),
            reply_body: status.reply_body,
            exit_code: status.exit_code,
            report: status.report,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn method_digest_text() {
        // An upload's intent digests the WIT method's text; the node's copy writes the same.
        use services::Method;
        let methods = [
            (Method::Get, "Method::Get"),
            (Method::Head, "Method::Head"),
            (Method::Post, "Method::Post"),
            (Method::Put, "Method::Put"),
            (Method::Patch, "Method::Patch"),
            (Method::Delete, "Method::Delete"),
        ];
        for (method, text) in methods {
            assert_eq!(format!("{method:?}"), text);
            assert_eq!(format!("{:?}", calls::Method::from(method)), text);
        }
    }
}
