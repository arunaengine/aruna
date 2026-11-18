//! Queries published repository records through the caller's connector authority.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::operation::Operation;
use aruna_core::repository::RepositoryQuery;
use aruna_core::repository::invenio::validate_id;
use aruna_core::structs::execution::harvest::RepositoryConnectorKind;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::secondary_id::normalize_doi;
use serde_json::Value;
use ulid::Ulid;
use url::Url;

use super::{Session, invalid, json};
use crate::jobs::repository::connector::{ConnectOperation, ConnectRequest};
use crate::jobs::repository::{NodeScope, TransferError, not_supported};

fn read_connect(
    scope: NodeScope,
    auth: AuthContext,
    group_id: Ulid,
    connector_id: Ulid,
    limit: u64,
) -> IoCall {
    IoCall::child(ConnectOperation::new(ConnectRequest {
        scope,
        auth,
        group_id,
        connector_id,
        permission: Permission::READ,
        limit,
        credential: None,
        stop: None,
    }))
}

/// Searches the repository's records with its native query syntax.
#[derive(Debug, PartialEq)]
pub struct SearchRecordsOperation {
    kind: RepositoryConnectorKind,
    scope: NodeScope,
    auth: AuthContext,
    query: RepositoryQuery,
    limit: u64,
    connected: bool,
}

impl SearchRecordsOperation {
    pub fn new(
        kind: RepositoryConnectorKind,
        scope: NodeScope,
        auth: AuthContext,
        query: RepositoryQuery,
        limit: u64,
    ) -> Self {
        Self {
            kind,
            scope,
            auth,
            query,
            limit,
            connected: false,
        }
    }
}

impl Operation for SearchRecordsOperation {
    type Output = Value;
    type Error = TransferError;

    fn first(&mut self) -> Result<Next<Value>, TransferError> {
        if self.kind != RepositoryConnectorKind::Invenio {
            return Err(not_supported("search"));
        }
        let query = &self.query;
        if query.page == 0 || !(1..=25).contains(&query.size) || query.q.len() > 4096 {
            return Err(invalid("invalid repository search bounds"));
        }
        Ok(Next::Call(read_connect(
            self.scope.clone(),
            self.auth.clone(),
            query.group_id,
            query.connector_id,
            self.limit,
        )))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Value>, TransferError> {
        if !self.connected {
            let session = result.child::<Session, TransferError>()??;
            let query = &self.query;
            let mut url = session.url(&["records"])?;
            url.query_pairs_mut()
                .append_pair("q", &query.q)
                .append_pair("page", &query.page.to_string())
                .append_pair("size", &query.size.to_string())
                .append_pair(
                    "allversions",
                    if query.all_versions { "true" } else { "false" },
                );
            self.connected = true;
            return Ok(Next::Call(session.get(url)));
        }
        let page = json(result)?;
        if !page["hits"]["hits"].is_array() || page["hits"]["total"].is_null() {
            return Err(invalid("invalid repository search response"));
        }
        Ok(Next::Done(page))
    }
}

/// A published record named by its id, a version or concept DOI, or its page or API URL.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RecordReference {
    Id(String),
    Doi(String),
    Url(String),
}

/// One search for a DOI: the indexed field, whether all versions count, and the DOI's form.
#[derive(Debug, PartialEq)]
struct DoiSearch {
    field: &'static str,
    all_versions: &'static str,
    form: String,
}

/// The record id a reference names in the connector's repository. A concept DOI names the
/// latest version; a URL must be on the repository's origin.
#[derive(Debug, PartialEq)]
pub struct ResolveRecordOperation {
    kind: RepositoryConnectorKind,
    scope: NodeScope,
    auth: AuthContext,
    group_id: Ulid,
    connector_id: Ulid,
    reference: RecordReference,
    limit: u64,
    session: Option<Session>,
    doi: String,
    searches: Vec<DoiSearch>,
}

impl ResolveRecordOperation {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        kind: RepositoryConnectorKind,
        scope: NodeScope,
        auth: AuthContext,
        group_id: Ulid,
        connector_id: Ulid,
        reference: RecordReference,
        limit: u64,
    ) -> Self {
        Self {
            kind,
            scope,
            auth,
            group_id,
            connector_id,
            reference,
            limit,
            session: None,
            doi: String::new(),
            searches: Vec::new(),
        }
    }

    fn found(id: String) -> Result<Next<String>, TransferError> {
        validate_id(&id)?;
        Ok(Next::Done(id))
    }

    /// The next search; the last one is taken first from the end of the list.
    fn search(&mut self) -> Result<Next<String>, TransferError> {
        let session = self
            .session
            .as_ref()
            .ok_or_else(|| invalid("repository session missing"))?;
        let Some(search) = self.searches.last() else {
            return Err(invalid("no published record has this DOI"));
        };
        let mut url = session.url(&["records"])?;
        url.query_pairs_mut()
            .append_pair(
                "q",
                &format!("{}.doi.identifier:\"{}\"", search.field, search.form),
            )
            .append_pair("size", "1")
            .append_pair("allversions", search.all_versions);
        Ok(Next::Call(session.get(url)))
    }
}

/// The searches for a DOI: the index matches exact case, so it is looked up as given and
/// lowercased. A version DOI names one version; a concept DOI names the lineage and its latest.
fn doi_searches(raw: &str, doi: &str) -> Vec<DoiSearch> {
    let trimmed = raw.trim();
    let given = trimmed
        .get(trimmed.len().saturating_sub(doi.len())..)
        .filter(|given| *given != doi && given.to_lowercase() == doi);
    let mut searches = Vec::new();
    for form in given.into_iter().chain([doi]) {
        for (field, all_versions) in [("pids", "true"), ("parent.pids", "false")] {
            searches.push(DoiSearch {
                field,
                all_versions,
                form: form.to_string(),
            });
        }
    }
    searches.reverse();
    searches
}

impl Operation for ResolveRecordOperation {
    type Output = String;
    type Error = TransferError;

    fn first(&mut self) -> Result<Next<String>, TransferError> {
        if self.kind != RepositoryConnectorKind::Invenio {
            return Err(not_supported("record lookup"));
        }
        Ok(Next::Call(read_connect(
            self.scope.clone(),
            self.auth.clone(),
            self.group_id,
            self.connector_id,
            self.limit,
        )))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<String>, TransferError> {
        let Some(search) = self.session.is_some().then(|| self.searches.pop()) else {
            let session = result.child::<Session, TransferError>()??;
            let reference = self.reference.clone();
            let endpoint = session.endpoint().to_string();
            self.session = Some(session);
            return match reference {
                RecordReference::Id(id) => Self::found(id),
                RecordReference::Url(url) => Self::found(url_record(&endpoint, &url)?),
                RecordReference::Doi(raw) => {
                    let doi = normalize_doi(&raw)
                        .ok()
                        .filter(|doi| !doi.contains(['"', '\\']))
                        .ok_or_else(|| invalid("invalid DOI"))?;
                    self.searches = doi_searches(&raw, &doi);
                    self.doi = doi;
                    self.search()
                }
            };
        };
        let search = search.ok_or_else(|| invalid("no published record has this DOI"))?;
        let page = json(result)?;
        let hit = &page["hits"]["hits"][0];
        let named = match search.field {
            "pids" => &hit["pids"]["doi"]["identifier"],
            _ => &hit["parent"]["pids"]["doi"]["identifier"],
        };
        let matches = named
            .as_str()
            .is_some_and(|value| normalize_doi(value).is_ok_and(|value| value == self.doi));
        if matches {
            let id = hit["id"]
                .as_str()
                .ok_or_else(|| invalid("no published record has this DOI"))?;
            return Self::found(id.to_string());
        }
        self.search()
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jobs::repository::invenio::Session;
    use crate::jobs::repository::push::creator_auth;
    use crate::jobs::repository::test_support::{body, child, link, scope};
    use aruna_core::repository::remote::InvenioAccess;
    use serde_json::json;

    fn session() -> Session {
        let access = InvenioAccess::new("https://zenodo.org/api/", None, 1024).unwrap();
        Session::new(access, None)
    }

    fn auth() -> AuthContext {
        creator_auth(&scope(), &link())
    }

    fn query(size: u8) -> RepositoryQuery {
        RepositoryQuery {
            group_id: link().group_id,
            connector_id: link().connector_id,
            q: "title:a".into(),
            page: 1,
            size,
            all_versions: false,
        }
    }

    fn search(size: u8, kind: RepositoryConnectorKind) -> SearchRecordsOperation {
        SearchRecordsOperation::new(kind, scope(), auth(), query(size), 1024)
    }

    #[test]
    fn search_checks_answer() {
        let mut operation = search(5, RepositoryConnectorKind::Invenio);
        assert!(matches!(
            operation.first(),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        let url = "https://zenodo.org/api/records?q=title%3Aa&page=1&size=5&allversions=false";
        assert_eq!(
            operation.next(child::<_, TransferError>(Ok(session()))),
            Ok(Next::Call(session().get(Url::parse(url).unwrap())))
        );
        assert_eq!(
            operation.next(body(json!({"hits": {}}))),
            Err(invalid("invalid repository search response"))
        );
    }

    #[test]
    fn search_refuses_bounds() {
        let mut operation = search(26, RepositoryConnectorKind::Invenio);
        assert_eq!(
            operation.first(),
            Err(invalid("invalid repository search bounds"))
        );
        let mut operation = search(5, RepositoryConnectorKind::OaiPmh);
        assert_eq!(operation.first(), Err(not_supported("search")));
    }

    fn resolve(reference: RecordReference) -> ResolveRecordOperation {
        ResolveRecordOperation::new(
            RepositoryConnectorKind::Invenio,
            scope(),
            auth(),
            link().group_id,
            link().connector_id,
            reference,
            1024,
        )
    }

    #[test]
    fn resolve_reads_url() {
        let mut operation = resolve(RecordReference::Url(
            "https://zenodo.org/records/123".into(),
        ));
        operation.first().unwrap();
        assert_eq!(
            operation.next(child::<_, TransferError>(Ok(session()))),
            Ok(Next::Done("123".to_string()))
        );
    }

    #[test]
    fn doi_tries_field() {
        let mut operation = resolve(RecordReference::Doi("10.1234/ab".into()));
        operation.first().unwrap();
        let version = operation.next(child::<_, TransferError>(Ok(session())));
        let mut url = session().url(&["records"]).unwrap();
        url.query_pairs_mut()
            .append_pair("q", "pids.doi.identifier:\"10.1234/ab\"")
            .append_pair("size", "1")
            .append_pair("allversions", "true");
        assert_eq!(version, Ok(Next::Call(session().get(url))));
        assert!(matches!(
            operation.next(body(json!({"hits": {"hits": []}}))),
            Ok(Next::Call(IoCall::Effect(_)))
        ));
        let hit = json!({"hits": {"hits": [{
            "id": "9",
            "parent": {"pids": {"doi": {"identifier": "10.1234/AB"}}},
        }]}});
        assert_eq!(operation.next(body(hit)), Ok(Next::Done("9".to_string())));
    }

    #[test]
    fn unknown_doi_fails() {
        let mut operation = resolve(RecordReference::Doi("10.1234/ab".into()));
        operation.first().unwrap();
        operation
            .next(child::<_, TransferError>(Ok(session())))
            .unwrap();
        operation.next(body(json!({"hits": {"hits": []}}))).unwrap();
        assert_eq!(
            operation.next(body(json!({"hits": {"hits": []}}))),
            Err(invalid("no published record has this DOI"))
        );
    }

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
