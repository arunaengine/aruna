//! OAI-PMH 2.0 data provider over the local realm's metadata registry.
//!
//! Read-only and unauthenticated. Records are enumerated through the anonymous
//! visibility index, which contains only documents an anonymous caller is
//! authorized to read; every candidate is re-checked before it is rendered, and
//! each record is exported through the routed holder path. GET and POST share
//! one duplicate-preserving argument parser and one per-verb argument matrix.

use std::sync::Arc;

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::{ConnectInfo, RawQuery, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::Response;
use axum::routing::get;
use base64::Engine;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use aruna_core::structs::{MetadataRegistryRecord, RealmId};
use aruna_operations::driver::DriverContext;
use aruna_operations::get_metadata_document::load_metadata_record_by_document;
use aruna_operations::harvest::oai::mapping::jsonld_to_oai_dc;
use aruna_operations::harvest::oai::request::format_from;
use aruna_operations::metadata::api::{
    ExportMetadataRoCrateRequest, ExportMetadataRoCrateResult, MetadataRoCrateExportView,
};
use aruna_operations::metadata::forward::export_rocrate_routed;
use aruna_operations::metadata::visibility_index::{VisibilityError, earliest_visible, visible_page};

use crate::forwarded::external_base_url;
use crate::server_state::ServerState;

/// OAI identifiers are the document graph IRI (`graph_iri_for`).
const GRAPH_IRI_PREFIX: &str = "https://w3id.org/aruna/";
const METADATA_PREFIX: &str = "oai_dc";
const PAGE_SIZE: usize = 100;
const REPOSITORY_NAME: &str = "Aruna";
const ADMIN_EMAIL: &str = "admin@localhost";
const OAI_PATH: &str = "/oai";
/// Only used when neither the configured API base URL nor a trusted proxy can
/// supply an absolute one; raw `Host` is never trusted.
const BASE_URL_FALLBACK: &str = "http://localhost/api/v1/oai";
const EARLIEST_FALLBACK: &str = "1970-01-01T00:00:00Z";
/// Worst-case growth of one metadata byte under XML entity escaping (`'` becomes
/// `&apos;`), applied to the per-document export cap to size the page budget.
const XML_ESCAPE_FACTOR: u64 = 6;
const OAI_DC_OPEN: &str = concat!(
    "<oai_dc:dc xmlns:oai_dc=\"http://www.openarchives.org/OAI/2.0/oai_dc/\" ",
    "xmlns:dc=\"http://purl.org/dc/elements/1.1/\" ",
    "xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" ",
    "xsi:schemaLocation=\"http://www.openarchives.org/OAI/2.0/oai_dc/ ",
    "http://www.openarchives.org/OAI/2.0/oai_dc.xsd\">"
);
const OAI_PMH_OPEN: &str = concat!(
    "<OAI-PMH xmlns=\"http://www.openarchives.org/OAI/2.0/\" ",
    "xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" ",
    "xsi:schemaLocation=\"http://www.openarchives.org/OAI/2.0/ ",
    "http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd\">"
);

pub fn router() -> Router<Arc<ServerState>> {
    Router::new().route(OAI_PATH, get(handle_oai).post(handle_oai_post))
}

/// The six protocol arguments, already validated against the verb's matrix.
#[derive(Debug, Default)]
struct OaiParams {
    verb: Option<String>,
    metadata_prefix: Option<String>,
    from: Option<String>,
    until: Option<String>,
    set: Option<String>,
    identifier: Option<String>,
    resumption_token: Option<String>,
}

/// A verb failure: an OAI protocol error (rendered in a 200 envelope), an
/// unavailable dependency (503), or an internal fault (500, no detail leaked).
#[derive(Debug)]
enum OaiFault {
    Protocol { code: &'static str, message: String },
    Unavailable,
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
    ConnectInfo(peer): ConnectInfo<std::net::SocketAddr>,
    headers: HeaderMap,
    RawQuery(query): RawQuery,
) -> Response {
    let base_url = base_url(&state, peer.ip(), &headers).await;
    let pairs = parse_pairs(query.unwrap_or_default().as_bytes());
    dispatch(state, base_url, pairs).await
}

async fn handle_oai_post(
    State(state): State<Arc<ServerState>>,
    ConnectInfo(peer): ConnectInfo<std::net::SocketAddr>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let base_url = base_url(&state, peer.ip(), &headers).await;
    if !is_form_encoded(&headers) {
        return respond(
            &base_url,
            &OaiParams::default(),
            Err(protocol(
                "badArgument",
                "POST requires application/x-www-form-urlencoded",
            )),
        );
    }
    let pairs = parse_pairs(body.as_ref());
    dispatch(state, base_url, pairs).await
}

fn is_form_encoded(headers: &HeaderMap) -> bool {
    headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(|value| {
            value
                .split(';')
                .next()
                .unwrap_or_default()
                .trim()
                .eq_ignore_ascii_case("application/x-www-form-urlencoded")
        })
        .unwrap_or(false)
}

/// Duplicate-preserving decode. Repeats must reach the verb matrix as
/// `badArgument` inside the OAI envelope rather than a bare transport 400.
fn parse_pairs(raw: &[u8]) -> Vec<(String, String)> {
    url::form_urlencoded::parse(raw)
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect()
}

async fn dispatch(
    state: Arc<ServerState>,
    base_url: String,
    pairs: Vec<(String, String)>,
) -> Response {
    let echo = echo_params(&pairs);
    let params = match validate_pairs(&pairs) {
        Ok(params) => params,
        Err(fault) => return respond(&base_url, &echo, Err(fault)),
    };
    let ctx = state.get_ctx();
    let realm_id = state.get_realm_id();
    let outcome = match params.verb.as_deref() {
        Some("Identify") => identify(ctx.as_ref(), &base_url).await,
        Some("ListMetadataFormats") => Ok(list_metadata_formats()),
        Some("ListSets") => Err(protocol(
            "noSetHierarchy",
            "This repository does not support sets",
        )),
        Some("ListIdentifiers") => list(&state, &ctx, realm_id, &params, false).await,
        Some("ListRecords") => list(&state, &ctx, realm_id, &params, true).await,
        Some("GetRecord") => get_record(&state, &ctx, realm_id, &params).await,
        _ => Err(protocol("badVerb", "Illegal OAI-PMH verb")),
    };
    respond(&base_url, &params, outcome)
}

/// Absolute endpoint URL. The configured API base URL wins; behind a trusted
/// proxy the forwarded base is the fallback. A raw `Host` header is never used.
async fn base_url(state: &ServerState, peer: std::net::IpAddr, headers: &HeaderMap) -> String {
    if let Some(rest) = state.interface_state().await.rest {
        return format!("{}{OAI_PATH}", rest.api_base_url.trim_end_matches('/'));
    }
    if crate::forwarded::peer_is_trusted(state.trusted_proxies(), peer) {
        let base = external_base_url(state.trusted_proxies(), peer, headers);
        return format!("{base}/api/v1{OAI_PATH}");
    }
    BASE_URL_FALLBACK.to_string()
}

const KNOWN_ARGS: [&str; 6] = [
    "identifier",
    "metadataPrefix",
    "from",
    "until",
    "set",
    "resumptionToken",
];

/// Arguments each verb accepts, and the ones it requires when no resumption
/// token is present.
fn verb_matrix(verb: &str) -> Option<(&'static [&'static str], &'static [&'static str])> {
    match verb {
        "Identify" => Some((&[], &[])),
        "ListMetadataFormats" => Some((&["identifier"], &[])),
        "ListSets" => Some((&["resumptionToken"], &[])),
        "ListIdentifiers" | "ListRecords" => Some((
            &["metadataPrefix", "from", "until", "set", "resumptionToken"],
            &["metadataPrefix"],
        )),
        "GetRecord" => Some((
            &["identifier", "metadataPrefix"],
            &["identifier", "metadataPrefix"],
        )),
        _ => None,
    }
}

/// The received arguments, unvalidated, for the `request` element of a response
/// that fails before validation.
fn echo_params(pairs: &[(String, String)]) -> OaiParams {
    let mut params = OaiParams::default();
    for (key, value) in pairs {
        let slot = match key.as_str() {
            "verb" => &mut params.verb,
            "identifier" => &mut params.identifier,
            "metadataPrefix" => &mut params.metadata_prefix,
            "from" => &mut params.from,
            "until" => &mut params.until,
            "set" => &mut params.set,
            "resumptionToken" => &mut params.resumption_token,
            _ => continue,
        };
        if slot.is_none() {
            *slot = Some(value.clone());
        }
    }
    params
}

fn validate_pairs(pairs: &[(String, String)]) -> Result<OaiParams, OaiFault> {
    let mut params = OaiParams::default();
    let mut seen: Vec<&str> = Vec::new();
    for (key, value) in pairs {
        if seen.contains(&key.as_str()) {
            return Err(protocol("badArgument", format!("Repeated argument: {key}")));
        }
        seen.push(key.as_str());
        if key == "verb" {
            params.verb = Some(value.clone());
            continue;
        }
        if !KNOWN_ARGS.contains(&key.as_str()) {
            return Err(protocol("badArgument", format!("Unknown argument: {key}")));
        }
    }

    let Some(verb) = params.verb.clone() else {
        return Err(protocol("badVerb", "Missing verb argument"));
    };
    let Some((allowed, required)) = verb_matrix(&verb) else {
        return Err(protocol("badVerb", "Illegal OAI-PMH verb"));
    };
    for (key, value) in pairs {
        if key == "verb" {
            continue;
        }
        if !allowed.contains(&key.as_str()) {
            return Err(protocol(
                "badArgument",
                format!("Argument {key} is not allowed for {verb}"),
            ));
        }
        match key.as_str() {
            "identifier" => params.identifier = Some(value.clone()),
            "metadataPrefix" => params.metadata_prefix = Some(value.clone()),
            "from" => params.from = Some(value.clone()),
            "until" => params.until = Some(value.clone()),
            "set" => params.set = Some(value.clone()),
            "resumptionToken" => params.resumption_token = Some(value.clone()),
            _ => {}
        }
    }
    if params.resumption_token.is_none() {
        for key in required {
            let present = match *key {
                "identifier" => params.identifier.is_some(),
                "metadataPrefix" => params.metadata_prefix.is_some(),
                _ => true,
            };
            if !present {
                return Err(protocol("badArgument", format!("{key} is required")));
            }
        }
    }
    Ok(params)
}

async fn identify(ctx: &DriverContext, base_url: &str) -> Result<String, OaiFault> {
    let earliest = match earliest_visible(ctx).await {
        Ok(Some(updated_at_ms)) => {
            format_from(updated_at_ms).unwrap_or_else(|| EARLIEST_FALLBACK.to_string())
        }
        Ok(None) => EARLIEST_FALLBACK.to_string(),
        Err(error) => return Err(visibility_fault(error)),
    };
    Ok(format!(
        "<Identify><repositoryName>{REPOSITORY_NAME}</repositoryName>\
         <baseURL>{}</baseURL><protocolVersion>2.0</protocolVersion>\
         <adminEmail>{ADMIN_EMAIL}</adminEmail>\
         <earliestDatestamp>{earliest}</earliestDatestamp>\
         <deletedRecord>no</deletedRecord>\
         <granularity>YYYY-MM-DDThh:mm:ssZ</granularity></Identify>",
        escape_xml(base_url)
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

fn visibility_fault(error: VisibilityError) -> OaiFault {
    match error {
        VisibilityError::Unavailable => OaiFault::Unavailable,
        VisibilityError::Storage(_) => OaiFault::Internal,
    }
}

async fn list(
    state: &ServerState,
    ctx: &Arc<DriverContext>,
    realm_id: RealmId,
    params: &OaiParams,
    include_metadata: bool,
) -> Result<String, OaiFault> {
    let (from_ms, until_ms, start_cursor) = resolve_window(params)?;
    // One entry beyond the page is the visible lookahead: a token is only issued
    // when another record actually exists.
    let page = visible_page(ctx.as_ref(), from_ms, until_ms, start_cursor, PAGE_SIZE + 1)
        .await
        .map_err(visibility_fault)?;
    if page.entries.is_empty() {
        return Err(protocol("noRecordsMatch", "No records match the request"));
    }

    let budget = state
        .rocrate_limits()
        .metadata_bytes
        .saturating_mul(XML_ESCAPE_FACTOR) as usize;
    let tag = if include_metadata {
        "ListRecords"
    } else {
        "ListIdentifiers"
    };
    let mut body = format!("<{tag}>");
    let mut emitted = 0usize;
    let mut last_cursor = None;
    let mut budget_stopped = false;
    for (cursor, record) in page.entries.iter().take(PAGE_SIZE) {
        let rendered = if include_metadata {
            render_record(state, ctx, realm_id, record).await?
        } else {
            render_header(record)
        };
        if body.len() + rendered.len() > budget {
            if emitted == 0 {
                return Err(OaiFault::Unavailable);
            }
            budget_stopped = true;
            break;
        }
        body.push_str(&rendered);
        emitted += 1;
        last_cursor = Some(cursor.clone());
    }

    let more = budget_stopped || page.entries.len() > emitted || page.budget_hit;
    if more {
        let cursor = last_cursor.expect("a token needs an emitted record");
        body.push_str(&format!(
            "<resumptionToken>{}</resumptionToken>",
            escape_xml(&encode_token(until_ms, cursor.as_ref().to_vec()))
        ));
    } else if params.resumption_token.is_some() {
        // The final response of a token-driven sequence carries an empty token.
        body.push_str("<resumptionToken />");
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
    let from = parse_bound(params.from.as_deref(), false)?;
    let until = parse_bound(params.until.as_deref(), true)?;
    if let (Some((_, from_day)), Some((_, until_day))) = (from, until)
        && from_day != until_day
    {
        return Err(protocol(
            "badArgument",
            "from and until must use the same granularity",
        ));
    }
    let from_ms = from.map(|(ms, _)| ms).unwrap_or(0);
    let until_ms = until.map(|(ms, _)| ms).unwrap_or(u64::MAX);
    if from_ms > until_ms {
        return Err(protocol("badArgument", "from must not be after until"));
    }
    Ok((from_ms, until_ms, None))
}

/// Parses an OAI datestamp and returns its bound in milliseconds plus whether it
/// was date-only. An inclusive `until` covers the whole represented interval.
fn parse_bound(value: Option<&str>, inclusive: bool) -> Result<Option<(u64, bool)>, OaiFault> {
    let Some(text) = value else {
        return Ok(None);
    };
    let invalid = || protocol("badArgument", "Datestamp is not valid");
    if let Ok(date) = chrono::NaiveDate::parse_from_str(text, "%Y-%m-%d") {
        let start = date
            .and_hms_opt(0, 0, 0)
            .ok_or_else(invalid)?
            .and_utc()
            .timestamp_millis();
        let start = u64::try_from(start).map_err(|_| invalid())?;
        let bound = if inclusive {
            start.saturating_add(86_400_000 - 1)
        } else {
            start
        };
        return Ok(Some((bound, true)));
    }
    let instant = chrono::DateTime::parse_from_rfc3339(text).map_err(|_| invalid())?;
    let millis = u64::try_from(instant.timestamp_millis()).map_err(|_| invalid())?;
    // Seconds-granularity datestamps name a whole second; an inclusive upper
    // bound must cover all of it.
    let bound = if inclusive {
        millis - millis % 1000 + 999
    } else {
        millis
    };
    Ok(Some((bound, false)))
}

async fn get_record(
    state: &ServerState,
    ctx: &Arc<DriverContext>,
    realm_id: RealmId,
    params: &OaiParams,
) -> Result<String, OaiFault> {
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
    let record = load_metadata_record_by_document(ctx.as_ref(), document_id)
        .await
        .map_err(|_| OaiFault::Internal)?
        // A record from another realm is not this repository's to serve, and a
        // public flag alone never crosses that boundary.
        .filter(|record| record.realm_id == realm_id && record.document_id == document_id);
    let Some(record) = record else {
        return Err(protocol("idDoesNotExist", "Unknown identifier"));
    };
    if !anon_can_read(ctx.as_ref(), &record).await? {
        return Err(protocol("idDoesNotExist", "Unknown identifier"));
    }
    Ok(format!(
        "<GetRecord>{}</GetRecord>",
        render_record(state, ctx, realm_id, &record).await?
    ))
}

/// Whether an anonymous caller may read this record, evaluated through the same
/// seam the visibility index is built on.
async fn anon_can_read(
    ctx: &DriverContext,
    record: &MetadataRegistryRecord,
) -> Result<bool, OaiFault> {
    aruna_operations::metadata::visibility_index::anon_readable(ctx, record)
        .await
        .map_err(visibility_fault)
}

fn render_header(record: &MetadataRegistryRecord) -> String {
    format!(
        "<header><identifier>{}</identifier><datestamp>{}</datestamp></header>",
        escape_xml(&record.graph_iri),
        datestamp(record)
    )
}

async fn render_record(
    state: &ServerState,
    ctx: &Arc<DriverContext>,
    realm_id: RealmId,
    record: &MetadataRegistryRecord,
) -> Result<String, OaiFault> {
    let jsonld = read_jsonld(state, ctx, realm_id, record.document_id).await?;
    let mut elements = String::new();
    for (element, value) in jsonld_to_oai_dc(&jsonld, &record.graph_iri) {
        elements.push_str(&format!(
            "<dc:{element}>{}</dc:{element}>",
            escape_xml(&value)
        ));
    }
    Ok(format!(
        "<record>{}<metadata>{OAI_DC_OPEN}{elements}</oai_dc:dc></metadata></record>",
        render_header(record)
    ))
}

/// Read the document's RO-Crate JSON-LD through the routed export path, so a
/// non-holder serves the holder's bytes. An unavailable holder is a service
/// failure; substituting empty or synthesized metadata is never acceptable.
async fn read_jsonld(
    state: &ServerState,
    ctx: &Arc<DriverContext>,
    realm_id: RealmId,
    document_id: Ulid,
) -> Result<String, OaiFault> {
    let request = ExportMetadataRoCrateRequest {
        document_id,
        auth: None,
        view: MetadataRoCrateExportView::Full,
        limit: None,
        offset: None,
        after: None,
    };
    match export_rocrate_routed(
        ctx,
        realm_id,
        request,
        None,
        state.rocrate_limits().metadata_bytes,
    )
    .await
    {
        Ok(ExportMetadataRoCrateResult::Full { jsonld, .. }) => Ok(jsonld),
        Ok(_) => Err(OaiFault::Internal),
        Err(_) => Err(OaiFault::Unavailable),
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

fn respond(base_url: &str, params: &OaiParams, outcome: Result<String, OaiFault>) -> Response {
    let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let request_url = escape_xml(base_url);
    let inner = match &outcome {
        Ok(body) => format!(
            "<request{}>{request_url}</request>{body}",
            request_attrs(params, false)
        ),
        Err(OaiFault::Protocol { code, message }) => {
            let omit = matches!(*code, "badVerb" | "badArgument");
            format!(
                "<request{}>{request_url}</request><error code=\"{code}\">{}</error>",
                request_attrs(params, omit),
                escape_xml(message)
            )
        }
        Err(OaiFault::Unavailable) => return status_response(StatusCode::SERVICE_UNAVAILABLE),
        Err(OaiFault::Internal) => return status_response(StatusCode::INTERNAL_SERVER_ERROR),
    };
    let xml = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         {OAI_PMH_OPEN}<responseDate>{now}</responseDate>{inner}</OAI-PMH>"
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

fn status_response(status: StatusCode) -> Response {
    let mut response = Response::new(Body::from("service error"));
    *response.status_mut() = status;
    response
}

/// Escapes the five XML entities and drops characters XML 1.0 cannot represent,
/// so a conforming harvester can always parse the response.
fn escape_xml(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '"' => escaped.push_str("&quot;"),
            '\'' => escaped.push_str("&apos;"),
            '\t' | '\n' | '\r' => escaped.push(character),
            '\u{20}'..='\u{D7FF}' | '\u{E000}'..='\u{FFFD}' | '\u{10000}'..='\u{10FFFF}' => {
                escaped.push(character)
            }
            _ => {}
        }
    }
    escaped
}

/// Opaque resumption-token payload: the fixed `until` bound, the metadata prefix,
/// and the visibility-index cursor to resume after.
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

    fn params(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect()
    }

    fn fault_code(fault: OaiFault) -> String {
        match fault {
            OaiFault::Protocol { code, .. } => code.to_string(),
            OaiFault::Unavailable => "unavailable".to_string(),
            OaiFault::Internal => "internal".to_string(),
        }
    }

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

    #[test]
    fn strips_illegal_xml_characters() {
        let value = "a\u{0}b\u{8}c\u{1F}d\u{B}e\tf\nh";
        assert_eq!(escape_xml(value), "abcde\tf\nh");
        assert_eq!(escape_xml("\u{FFFE}\u{FFFF}ok"), "ok");
    }

    #[test]
    fn duplicate_argument_is_bad() {
        let error = validate_pairs(&params(&[
            ("verb", "GetRecord"),
            ("identifier", "a"),
            ("identifier", "b"),
        ]))
        .err()
        .unwrap();
        assert_eq!(fault_code(error), "badArgument");
    }

    #[test]
    fn unknown_argument_is_bad() {
        let error = validate_pairs(&params(&[("verb", "Identify"), ("nonsense", "1")]))
            .err()
            .unwrap();
        assert_eq!(fault_code(error), "badArgument");
    }

    #[test]
    fn illegal_verb_is_bad() {
        assert_eq!(
            fault_code(validate_pairs(&params(&[("verb", "Nope")])).err().unwrap()),
            "badVerb"
        );
        assert_eq!(
            fault_code(validate_pairs(&params(&[])).err().unwrap()),
            "badVerb"
        );
    }

    #[test]
    fn verb_matrix_rejects_extras() {
        // Identify takes no selective arguments; GetRecord takes no window.
        assert_eq!(
            fault_code(
                validate_pairs(&params(&[("verb", "Identify"), ("metadataPrefix", "oai_dc")]))
                    .err()
                    .unwrap()
            ),
            "badArgument"
        );
        assert_eq!(
            fault_code(
                validate_pairs(&params(&[
                    ("verb", "GetRecord"),
                    ("identifier", "x"),
                    ("metadataPrefix", "oai_dc"),
                    ("from", "2026-01-01"),
                ]))
                .err()
                .unwrap()
            ),
            "badArgument"
        );
    }

    #[test]
    fn missing_required_is_bad() {
        assert_eq!(
            fault_code(
                validate_pairs(&params(&[("verb", "ListRecords")]))
                    .err()
                    .unwrap()
            ),
            "badArgument"
        );
        assert_eq!(
            fault_code(
                validate_pairs(&params(&[
                    ("verb", "GetRecord"),
                    ("metadataPrefix", "oai_dc")
                ]))
                .err()
                .unwrap()
            ),
            "badArgument"
        );
    }

    #[test]
    fn token_only_needs_no_prefix() {
        let parsed = validate_pairs(&params(&[
            ("verb", "ListRecords"),
            ("resumptionToken", "abc"),
        ]))
        .unwrap();
        assert_eq!(parsed.resumption_token.as_deref(), Some("abc"));
    }

    #[test]
    fn form_pairs_keep_duplicates() {
        let pairs = parse_pairs(b"verb=ListRecords&from=a&from=b");
        assert_eq!(pairs.len(), 3);
        assert_eq!(fault_code(validate_pairs(&pairs).err().unwrap()), "badArgument");
    }

    #[test]
    fn until_covers_whole_second() {
        let params = OaiParams {
            verb: Some("ListRecords".to_string()),
            metadata_prefix: Some(METADATA_PREFIX.to_string()),
            until: Some("2026-01-02T03:04:05Z".to_string()),
            ..OaiParams::default()
        };
        let (from_ms, until_ms, _) = resolve_window(&params).unwrap();
        assert_eq!(from_ms, 0);
        let second = chrono::DateTime::parse_from_rfc3339("2026-01-02T03:04:05Z")
            .unwrap()
            .timestamp_millis() as u64;
        assert_eq!(until_ms, second + 999);
        // .000, .001 and .999 of that second all fall inside the bound.
        assert!(second <= until_ms && second + 1 <= until_ms && second + 999 <= until_ms);
    }

    #[test]
    fn until_covers_whole_day() {
        let params = OaiParams {
            verb: Some("ListRecords".to_string()),
            metadata_prefix: Some(METADATA_PREFIX.to_string()),
            from: Some("2026-01-02".to_string()),
            until: Some("2026-01-02".to_string()),
            ..OaiParams::default()
        };
        let (from_ms, until_ms, _) = resolve_window(&params).unwrap();
        assert_eq!(until_ms - from_ms, 86_400_000 - 1);
    }

    #[test]
    fn mixed_granularity_is_bad() {
        let params = OaiParams {
            verb: Some("ListRecords".to_string()),
            metadata_prefix: Some(METADATA_PREFIX.to_string()),
            from: Some("2026-01-02".to_string()),
            until: Some("2026-01-03T00:00:00Z".to_string()),
            ..OaiParams::default()
        };
        assert_eq!(
            fault_code(resolve_window(&params).err().unwrap()),
            "badArgument"
        );
    }

    #[test]
    fn token_excludes_selective_args() {
        let params = OaiParams {
            verb: Some("ListRecords".to_string()),
            metadata_prefix: Some(METADATA_PREFIX.to_string()),
            resumption_token: Some("t".to_string()),
            ..OaiParams::default()
        };
        assert_eq!(
            fault_code(resolve_window(&params).err().unwrap()),
            "badArgument"
        );
    }

    #[test]
    fn envelope_declares_schema() {
        let response = respond(
            "https://example.test/api/v1/oai",
            &OaiParams {
                verb: Some("Identify".to_string()),
                ..OaiParams::default()
            },
            Ok("<Identify />".to_string()),
        );
        assert_eq!(response.status(), StatusCode::OK);
        assert!(OAI_PMH_OPEN.contains("xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\""));
        assert!(OAI_PMH_OPEN.contains("xsi:schemaLocation="));
        assert!(OAI_PMH_OPEN.contains("OAI-PMH.xsd"));
        assert!(OAI_DC_OPEN.contains("xmlns:xsi="));
        assert!(OAI_DC_OPEN.contains("oai_dc.xsd"));
    }

    #[test]
    fn form_content_type_is_checked() {
        let mut headers = HeaderMap::new();
        assert!(!is_form_encoded(&headers));
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/x-www-form-urlencoded; charset=utf-8"),
        );
        assert!(is_form_encoded(&headers));
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        assert!(!is_form_encoded(&headers));
    }
}
