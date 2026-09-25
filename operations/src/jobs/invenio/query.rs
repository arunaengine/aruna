//! Queries published repository records through the caller's connector authority.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::invenio::{InvenioQuery, validate_id};
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::secondary_id::normalize_doi;
use http::Method;
use serde_json::Value;
use ulid::Ulid;
use url::Url;

use super::{TransferError, connect};
use crate::driver::DriverContext;

pub async fn search_records(
    context: &DriverContext,
    auth: &AuthContext,
    query: &InvenioQuery,
    limit: u64,
) -> Result<Value, TransferError> {
    if query.page == 0 || !(1..=25).contains(&query.size) || query.q.len() > 4096 {
        return Err(TransferError::Permanent(
            "invalid repository search bounds".into(),
        ));
    }
    let client = connect(
        context,
        auth,
        query.group_id,
        query.connector_id,
        Permission::READ,
        limit,
        None,
    )
    .await?;
    let mut url = client.url(&["records"])?;
    url.query_pairs_mut()
        .append_pair("q", &query.q)
        .append_pair("page", &query.page.to_string())
        .append_pair("size", &query.size.to_string())
        .append_pair(
            "allversions",
            if query.all_versions { "true" } else { "false" },
        );
    let page = client.json(Method::GET, url, None).await?;
    if !page["hits"]["hits"].is_array() || page["hits"]["total"].is_null() {
        return Err(TransferError::Permanent(
            "invalid repository search response".into(),
        ));
    }
    Ok(page)
}

/// A published record named by its id, a version or concept DOI, or its page or API URL.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RecordReference {
    Id(String),
    Doi(String),
    Url(String),
}

/// The record id a reference names in the connector's repository. A concept DOI names the
/// latest version; a URL must be on the repository's origin.
pub async fn resolve_record(
    context: &DriverContext,
    auth: &AuthContext,
    group_id: Ulid,
    connector_id: Ulid,
    reference: &RecordReference,
    limit: u64,
) -> Result<String, TransferError> {
    let client = connect(
        context,
        auth,
        group_id,
        connector_id,
        Permission::READ,
        limit,
        None,
    )
    .await?;
    let id = match reference {
        RecordReference::Id(id) => id.clone(),
        RecordReference::Url(url) => url_record(client.endpoint(), url)?,
        RecordReference::Doi(raw) => {
            let doi = normalize_doi(raw)
                .ok()
                .filter(|doi| !doi.contains(['"', '\\']))
                .ok_or_else(|| invalid("invalid DOI"))?;
            // The index matches exact case, so the DOI is looked up as given and lowercased.
            let trimmed = raw.trim();
            let given = trimmed
                .get(trimmed.len().saturating_sub(doi.len())..)
                .filter(|given| *given != doi && given.to_lowercase() == doi);
            let forms = given.into_iter().chain([doi.as_str()]);
            let searches = forms.flat_map(|form| {
                [("pids", "true"), ("parent.pids", "false")].map(|(field, all)| (field, all, form))
            });
            // A version DOI names one version; a concept DOI names the lineage and its latest.
            let mut found = None;
            for (field, all_versions, form) in searches {
                let mut url = client.url(&["records"])?;
                url.query_pairs_mut()
                    .append_pair("q", &format!("{field}.doi.identifier:\"{form}\""))
                    .append_pair("size", "1")
                    .append_pair("allversions", all_versions);
                let page = client.json(Method::GET, url, None).await?;
                let hit = &page["hits"]["hits"][0];
                let named = match field {
                    "pids" => &hit["pids"]["doi"]["identifier"],
                    _ => &hit["parent"]["pids"]["doi"]["identifier"],
                };
                if named
                    .as_str()
                    .is_some_and(|value| normalize_doi(value).is_ok_and(|value| value == doi))
                {
                    found = hit["id"].as_str().map(str::to_string);
                    break;
                }
            }
            found.ok_or_else(|| invalid("no published record has this DOI"))?
        }
    };
    validate_id(&id)?;
    Ok(id)
}

/// The record id in a `/records/{id}` or `/record/{id}` URL on the endpoint's origin.
fn url_record(endpoint: &str, url: &str) -> Result<String, TransferError> {
    let origin = Url::parse(endpoint).map_err(|_| invalid("invalid connector endpoint"))?;
    let url = Url::parse(url).map_err(|_| invalid("invalid record URL"))?;
    if url.origin() != origin.origin() {
        return Err(invalid(
            "the record URL is not on the connector's repository",
        ));
    }
    let segments = url
        .path_segments()
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    segments
        .windows(2)
        .find(|pair| matches!(pair[0], "records" | "record"))
        .map(|pair| pair[1].to_string())
        .ok_or_else(|| invalid("the URL names no record"))
}

fn invalid(message: &str) -> TransferError {
    TransferError::Permanent(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_record_urls() {
        let api = "https://zenodo.org/api/";
        for url in [
            "https://zenodo.org/records/123",
            "https://zenodo.org/record/123?download=1",
            "https://zenodo.org/api/records/123/versions/latest",
        ] {
            assert_eq!(url_record(api, url).unwrap(), "123", "{url}");
        }
        for url in [
            "https://sandbox.zenodo.org/records/123",
            "http://zenodo.org/records/123",
            "https://zenodo.org/communities/aruna",
            "not a url",
        ] {
            assert!(url_record(api, url).is_err(), "{url}");
        }
    }
}
