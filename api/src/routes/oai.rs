//! OAI-PMH 2.0 data provider over the local realm's public metadata registry.
//!
//! Read-only and unauthenticated: only `public` documents are exposed. Records
//! are enumerated by `updated_at_ms` via the registry timestamp index, and each
//! is rendered as `oai_dc` through the swappable `jsonld_to_oai_dc` crosswalk.

use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::extract::{Query, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::Response;
use axum::routing::get;
use base64::Engine;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use aruna_core::storage_entries::metadata_updated_index_key;
use aruna_core::structs::MetadataRegistryRecord;
use aruna_operations::driver::DriverContext;
use aruna_operations::get_metadata_document::load_metadata_record_by_document;
use aruna_operations::harvest::oai::mapping::jsonld_to_oai_dc;
use aruna_operations::harvest::oai::parse::parse_datestamp_ms;
use aruna_operations::harvest::oai::request::format_from;
use aruna_operations::metadata::timestamp_index::enumerate_updated;

use crate::server_state::ServerState;

/// OAI identifiers are the document graph IRI (`graph_iri_for`).
const GRAPH_IRI_PREFIX: &str = "https://w3id.org/aruna/";
const METADATA_PREFIX: &str = "oai_dc";
const PAGE_SIZE: usize = 100;
const REPOSITORY_NAME: &str = "Aruna";
const ADMIN_EMAIL: &str = "admin@localhost";
/// Mounted under the `/api/v1` nest; kept relative to avoid trusting proxy headers.
const BASE_URL: &str = "/api/v1/oai";
const EARLIEST_FALLBACK: &str = "1970-01-01T00:00:00Z";
const OAI_DC_OPEN: &str = concat!(
    "<oai_dc:dc xmlns:oai_dc=\"http://www.openarchives.org/OAI/2.0/oai_dc/\" ",
    "xmlns:dc=\"http://purl.org/dc/elements/1.1/\">"
);

pub fn router() -> Router<Arc<ServerState>> {
    Router::new().route("/oai", get(handle_oai))
}

#[derive(Debug, Deserialize)]
struct OaiParams {
    verb: Option<String>,
    #[serde(rename = "metadataPrefix")]
    metadata_prefix: Option<String>,
    from: Option<String>,
    until: Option<String>,
    set: Option<String>,
    identifier: Option<String>,
    #[serde(rename = "resumptionToken")]
    resumption_token: Option<String>,
}

/// A verb failure: an OAI protocol error (rendered in a 200 envelope) or an
/// internal fault (a 500, with no detail leaked).
enum OaiFault {
    Protocol { code: &'static str, message: String },
    Internal,
}

fn protocol(code: &'static str, message: impl Into<String>) -> OaiFault {
    OaiFault::Protocol {
        code,
        message: message.into(),
    }
}

async fn handle_oai(
    State(state): State<Arc<ServerState>>,
    Query(params): Query<OaiParams>,
) -> Response {
    let ctx = state.get_ctx();
    let outcome = match params.verb.as_deref() {
        Some("Identify") => identify(&ctx).await,
        Some("ListMetadataFormats") => Ok(list_metadata_formats()),
        Some("ListSets") => Err(protocol(
            "noSetHierarchy",
            "This repository does not support sets",
        )),
        Some("ListIdentifiers") => list(&ctx, &params, false).await,
        Some("ListRecords") => list(&ctx, &params, true).await,
        Some("GetRecord") => get_record(&ctx, &params).await,
        Some(_) => Err(protocol("badVerb", "Illegal OAI-PMH verb")),
        None => Err(protocol("badVerb", "Missing verb argument")),
    };
    respond(&params, outcome)
}

async fn identify(ctx: &DriverContext) -> Result<String, OaiFault> {
    let earliest = earliest_datestamp(ctx).await;
    Ok(format!(
        "<Identify><repositoryName>{REPOSITORY_NAME}</repositoryName>\
         <baseURL>{BASE_URL}</baseURL><protocolVersion>2.0</protocolVersion>\
         <adminEmail>{ADMIN_EMAIL}</adminEmail>\
         <earliestDatestamp>{earliest}</earliestDatestamp>\
         <deletedRecord>no</deletedRecord>\
         <granularity>YYYY-MM-DDThh:mm:ssZ</granularity></Identify>"
    ))
}

fn list_metadata_formats() -> String {
    "<ListMetadataFormats><metadataFormat>\
     <metadataPrefix>oai_dc</metadataPrefix>\
     <schema>http://www.openarchives.org/OAI/2.0/oai_dc.xsd</schema>\
     <metadataNamespace>http://www.openarchives.org/OAI/2.0/oai_dc/</metadataNamespace>\
     </metadataFormat></ListMetadataFormats>"
        .to_string()
}

async fn list(
    ctx: &DriverContext,
    params: &OaiParams,
    include_metadata: bool,
) -> Result<String, OaiFault> {
    let (from_ms, until_ms, start_cursor) = resolve_window(params)?;

    let mut collected: Vec<MetadataRegistryRecord> = Vec::new();
    let mut cursor = start_cursor;
    let mut next_token: Option<String> = None;

    'gather: loop {
        let page = enumerate_updated(ctx, from_ms, until_ms, cursor.clone(), PAGE_SIZE)
            .await
            .map_err(|_| OaiFault::Internal)?;
        for record in page.records {
            if !record.public {
                continue;
            }
            collected.push(record);
            if collected.len() >= PAGE_SIZE {
                let last = collected.last().expect("just pushed");
                let key = metadata_updated_index_key(last.updated_at_ms, last.document_id);
                next_token = Some(encode_token(until_ms, key.as_ref().to_vec()));
                break 'gather;
            }
        }
        match page.next_after {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }

    if collected.is_empty() {
        return Err(protocol("noRecordsMatch", "No records match the request"));
    }

    let tag = if include_metadata {
        "ListRecords"
    } else {
        "ListIdentifiers"
    };
    let mut body = format!("<{tag}>");
    for record in &collected {
        if include_metadata {
            body.push_str(&render_record(ctx, record).await);
        } else {
            body.push_str(&render_header(record));
        }
    }
    if let Some(token) = next_token {
        body.push_str(&format!(
            "<resumptionToken>{}</resumptionToken>",
            escape_xml(&token)
        ));
    }
    body.push_str(&format!("</{tag}>"));
    Ok(body)
}

/// Resolve the datestamp window and start cursor. A `resumptionToken` is
/// exclusive of every selective argument.
fn resolve_window(
    params: &OaiParams,
) -> Result<(u64, u64, Option<aruna_core::types::Key>), OaiFault> {
    if let Some(token) = params.resumption_token.as_deref() {
        if params.metadata_prefix.is_some()
            || params.from.is_some()
            || params.until.is_some()
            || params.set.is_some()
        {
            return Err(protocol(
                "badArgument",
                "resumptionToken must be the only argument",
            ));
        }
        let payload = decode_token(token)
            .filter(|payload| payload.prefix == METADATA_PREFIX)
            .ok_or_else(|| {
                protocol(
                    "badResumptionToken",
                    "The resumptionToken is invalid or expired",
                )
            })?;
        return Ok((
            0,
            payload.until_ms,
            Some(aruna_core::types::Key::from(payload.cursor)),
        ));
    }

    match params.metadata_prefix.as_deref() {
        None => return Err(protocol("badArgument", "metadataPrefix is required")),
        Some(METADATA_PREFIX) => {}
        Some(_) => {
            return Err(protocol(
                "cannotDisseminateFormat",
                "Only oai_dc is supported",
            ));
        }
    }
    if params.set.is_some() {
        return Err(protocol(
            "noSetHierarchy",
            "This repository does not support sets",
        ));
    }
    let from_ms = parse_bound(params.from.as_deref())?.unwrap_or(0);
    let until_ms = parse_bound(params.until.as_deref())?.unwrap_or(u64::MAX);
    Ok((from_ms, until_ms, None))
}

fn parse_bound(value: Option<&str>) -> Result<Option<u64>, OaiFault> {
    match value {
        None => Ok(None),
        Some(text) => parse_datestamp_ms(text)
            .map(Some)
            .ok_or_else(|| protocol("badArgument", "Datestamp is not valid")),
    }
}

async fn get_record(ctx: &DriverContext, params: &OaiParams) -> Result<String, OaiFault> {
    match params.metadata_prefix.as_deref() {
        None => return Err(protocol("badArgument", "metadataPrefix is required")),
        Some(METADATA_PREFIX) => {}
        Some(_) => {
            return Err(protocol(
                "cannotDisseminateFormat",
                "Only oai_dc is supported",
            ));
        }
    }
    let Some(identifier) = params.identifier.as_deref() else {
        return Err(protocol("badArgument", "identifier is required"));
    };
    let Some(document_id) = parse_identifier(identifier) else {
        return Err(protocol("idDoesNotExist", "Unknown identifier"));
    };
    let record = load_metadata_record_by_document(ctx, document_id)
        .await
        .map_err(|_| OaiFault::Internal)?
        .filter(|record| record.public);
    let Some(record) = record else {
        return Err(protocol("idDoesNotExist", "Unknown identifier"));
    };
    Ok(format!(
        "<GetRecord>{}</GetRecord>",
        render_record(ctx, &record).await
    ))
}

async fn earliest_datestamp(ctx: &DriverContext) -> String {
    match enumerate_updated(ctx, 0, u64::MAX, None, 1).await {
        Ok(page) => page
            .records
            .first()
            .and_then(|record| format_from(record.updated_at_ms))
            .unwrap_or_else(|| EARLIEST_FALLBACK.to_string()),
        Err(_) => EARLIEST_FALLBACK.to_string(),
    }
}

fn render_header(record: &MetadataRegistryRecord) -> String {
    format!(
        "<header><identifier>{}</identifier><datestamp>{}</datestamp></header>",
        escape_xml(&record.graph_iri),
        datestamp(record)
    )
}

async fn render_record(ctx: &DriverContext, record: &MetadataRegistryRecord) -> String {
    let jsonld = read_jsonld(ctx, &record.graph_iri).await;
    let mut elements = String::new();
    for (element, value) in jsonld_to_oai_dc(&jsonld, &record.graph_iri) {
        elements.push_str(&format!(
            "<dc:{element}>{}</dc:{element}>",
            escape_xml(&value)
        ));
    }
    format!(
        "<record>{}<metadata>{OAI_DC_OPEN}{elements}</oai_dc:dc></metadata></record>",
        render_header(record)
    )
}

async fn read_jsonld(ctx: &DriverContext, graph_iri: &str) -> String {
    match ctx.metadata_handle.as_ref() {
        Some(handle) => handle
            .export_rocrate_jsonld(graph_iri.to_string())
            .await
            .unwrap_or_default(),
        None => String::new(),
    }
}

fn datestamp(record: &MetadataRegistryRecord) -> String {
    format_from(record.updated_at_ms).unwrap_or_else(|| EARLIEST_FALLBACK.to_string())
}

fn parse_identifier(identifier: &str) -> Option<Ulid> {
    identifier
        .strip_prefix(GRAPH_IRI_PREFIX)
        .and_then(|id| Ulid::from_string(id).ok())
}

fn respond(params: &OaiParams, outcome: Result<String, OaiFault>) -> Response {
    let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let inner = match &outcome {
        Ok(body) => format!(
            "<request{}>{BASE_URL}</request>{body}",
            request_attrs(params, false)
        ),
        Err(OaiFault::Protocol { code, message }) => {
            let omit = matches!(*code, "badVerb" | "badArgument");
            format!(
                "<request{}>{BASE_URL}</request><error code=\"{code}\">{}</error>",
                request_attrs(params, omit),
                escape_xml(message)
            )
        }
        Err(OaiFault::Internal) => return internal_response(),
    };
    let xml = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <OAI-PMH xmlns=\"http://www.openarchives.org/OAI/2.0/\">\
         <responseDate>{now}</responseDate>{inner}</OAI-PMH>"
    );
    xml_response(xml)
}

/// Echo the received arguments as `request` attributes, except on `badVerb`/
/// `badArgument`, where the spec requires the bare base URL.
fn request_attrs(params: &OaiParams, omit: bool) -> String {
    if omit {
        return String::new();
    }
    let mut attrs = String::new();
    let fields: [(&str, &Option<String>); 7] = [
        ("verb", &params.verb),
        ("identifier", &params.identifier),
        ("metadataPrefix", &params.metadata_prefix),
        ("from", &params.from),
        ("until", &params.until),
        ("set", &params.set),
        ("resumptionToken", &params.resumption_token),
    ];
    for (key, value) in fields {
        if let Some(value) = value {
            attrs.push_str(&format!(" {key}=\"{}\"", escape_xml(value)));
        }
    }
    attrs
}

fn xml_response(xml: String) -> Response {
    let mut response = Response::new(Body::from(xml));
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/xml; charset=utf-8"),
    );
    response
}

fn internal_response() -> Response {
    let mut response = Response::new(Body::from("internal error"));
    *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
    response
}

fn escape_xml(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

/// Opaque resumption-token payload: the fixed `until` bound, the metadata prefix,
/// and the timestamp-index cursor to resume after.
#[derive(Serialize, Deserialize)]
struct TokenPayload {
    until_ms: u64,
    prefix: String,
    cursor: Vec<u8>,
}

fn encode_token(until_ms: u64, cursor: Vec<u8>) -> String {
    let payload = TokenPayload {
        until_ms,
        prefix: METADATA_PREFIX.to_string(),
        cursor,
    };
    let bytes = postcard::to_allocvec(&payload).expect("postcard token is infallible");
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

fn decode_token(token: &str) -> Option<TokenPayload> {
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(token)
        .ok()?;
    postcard::from_bytes(&bytes).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identifier_round_trips_graph_iri() {
        let id = Ulid::from_bytes([7; 16]);
        let iri = format!("{GRAPH_IRI_PREFIX}{id}");
        assert_eq!(parse_identifier(&iri), Some(id));
        assert_eq!(parse_identifier("oai:other:1"), None);
    }

    #[test]
    fn token_round_trips() {
        let cursor = vec![1, 2, 3, 4];
        let encoded = encode_token(42, cursor.clone());
        let decoded = decode_token(&encoded).unwrap();
        assert_eq!(decoded.until_ms, 42);
        assert_eq!(decoded.prefix, METADATA_PREFIX);
        assert_eq!(decoded.cursor, cursor);
        assert!(decode_token("!!!not base64!!!").is_none());
    }

    #[test]
    fn escapes_xml_special_characters() {
        assert_eq!(
            escape_xml("a & b < c > \"d\" 'e'"),
            "a &amp; b &lt; c &gt; &quot;d&quot; &apos;e&apos;"
        );
    }
}
