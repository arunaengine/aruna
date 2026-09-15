//! Read-only OAI-PMH 2.0 provider over the anonymous metadata visibility index.
//! Candidates are reauthorized and exported through their routed holder path.
//! GET and POST share duplicate-preserving parsing with verb-specific validation.

use std::sync::Arc;

use axum::body::{Body, Bytes};
use axum::extract::{ConnectInfo, RawQuery, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::Response;
use base64::Engine;
use serde::{Deserialize, Serialize};
use ulid::Ulid;
use utoipa::OpenApi;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use aruna_core::structs::{MetadataRegistryRecord, RealmId};
use aruna_operations::driver::DriverContext;
use aruna_operations::harvest::oai_pmh::mapping::jsonld_to_dc;
use aruna_operations::harvest::oai_pmh::request::format_from;
use aruna_operations::metadata::api::{
    ExportMetadataRequest, ExportMetadataResult, RoCrateExportView,
};
use aruna_operations::metadata::forward::export_rocrate_routed;
use aruna_operations::metadata::get_document::load_document_record;
use aruna_operations::metadata::visibility_index::{
    VisibilityError, earliest_visible, effective_datestamp, visible_page,
};

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

#[derive(OpenApi)]
#[openapi(
    tags((name = "oai", description = "OAI-PMH 2.0 metadata harvesting"))
)]
pub struct OaiApiDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(OaiApiDoc::openapi()).routes(routes!(handle_oai, handle_oai_post))
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

#[utoipa::path(
    get,
    path = "/oai",
    tag = "oai",
    summary = "Answer an OAI-PMH request from query arguments",
    description = r#"Public OAI-PMH 2.0 provider for this realm's metadata registry, driven by query arguments.

**Authentication**: none; a token is neither required nor accepted. Only documents an anonymous
caller may read are enumerated, and every candidate is re-checked before it is rendered.

**Behavior**
- Supported verbs are `Identify`, `ListMetadataFormats`, `ListSets`, `ListIdentifiers`,
  `ListRecords` and `GetRecord`.
- `oai_dc` is the only disseminated format and the repository has no set hierarchy.
- Lists are paged at 100 records and continue through `resumptionToken`, which must then be the
  only argument; the last response of a token sequence carries an empty `resumptionToken` element.
- Protocol failures (`badVerb`, `badArgument`, `cannotDisseminateFormat`, `idDoesNotExist`,
  `noRecordsMatch`, `noSetHierarchy`, `badResumptionToken`) are error elements inside the 200
  envelope, not HTTP error codes."#,
    params(
        ("verb" = Option<String>, Query, description = "OAI-PMH verb; a missing or unknown verb answers badVerb"),
        ("metadataPrefix" = Option<String>, Query, description = "Requested metadata format, required by ListIdentifiers, ListRecords and GetRecord unless a resumptionToken is used. Only oai_dc is supported; anything else answers cannotDisseminateFormat"),
        ("identifier" = Option<String>, Query, description = "Record identifier, the document graph IRI https://w3id.org/aruna/{document_id}. Required by GetRecord, optional for ListMetadataFormats"),
        ("from" = Option<String>, Query, description = "Inclusive lower datestamp bound as YYYY-MM-DD or YYYY-MM-DDThh:mm:ssZ; both bounds must use the same granularity"),
        ("until" = Option<String>, Query, description = "Inclusive upper datestamp bound in the same forms. When omitted the window closes at the current instant, so later writes cannot join a running sequence"),
        ("set" = Option<String>, Query, description = "Set spec; this repository publishes no sets and answers noSetHierarchy"),
        ("resumptionToken" = Option<String>, Query, description = "Continuation token from a previous incomplete list. It must be the only argument, and an unknown or expired token answers badResumptionToken")
    ),
    responses(
        (
            status = 200,
            description = "OAI-PMH envelope as text/xml; verb results and protocol errors both use this status",
            body = String,
            content_type = "text/xml",
            example = json!("<?xml version=\"1.0\" encoding=\"UTF-8\"?><OAI-PMH xmlns=\"http://www.openarchives.org/OAI/2.0/\"><responseDate>2026-04-09T14:23:11Z</responseDate><request verb=\"Identify\">https://node.example.test/api/v1/oai</request><Identify><repositoryName>Aruna</repositoryName></Identify></OAI-PMH>")
        ),
        (status = 500, description = "Internal fault; the body is a plain-text marker, not an OAI-PMH envelope"),
        (status = 503, description = "The anonymous visibility index is unavailable; retryable. The body is a plain-text marker, not an OAI-PMH envelope")
    ),
    security(())
)]
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

#[utoipa::path(
    post,
    path = "/oai",
    tag = "oai",
    summary = "Answer an OAI-PMH request from a url-encoded form",
    description = r#"The POST form of the same public OAI-PMH provider, taking arguments as a url-encoded body.

**Authentication**: none; a token is neither required nor accepted, and only anonymously readable
documents are enumerated.

**Behavior**
- Arguments arrive as an `application/x-www-form-urlencoded` body instead of a query string and are
  validated by the identical verb matrix, so protocol failures again ride inside the 200 envelope.
- Any other content type answers a `badArgument` envelope, as does a repeated argument, which is
  never silently collapsed."#,
    request_body(
        content = String,
        content_type = "application/x-www-form-urlencoded",
        description = "The same OAI-PMH arguments as the GET form, url-encoded, for example verb=ListRecords&metadataPrefix=oai_dc"
    ),
    responses(
        (
            status = 200,
            description = "OAI-PMH envelope as text/xml; verb results and protocol errors both use this status",
            body = String,
            content_type = "text/xml",
            example = json!("<?xml version=\"1.0\" encoding=\"UTF-8\"?><OAI-PMH xmlns=\"http://www.openarchives.org/OAI/2.0/\"><responseDate>2026-04-09T14:23:11Z</responseDate><request verb=\"ListIdentifiers\" metadataPrefix=\"oai_dc\">https://node.example.test/api/v1/oai</request><ListIdentifiers><header><identifier>https://w3id.org/aruna/01JMETADATA0123456789ABCDE</identifier><datestamp>2026-04-09T14:23:11Z</datestamp></header></ListIdentifiers></OAI-PMH>")
        ),
        (status = 500, description = "Internal fault; the body is a plain-text marker, not an OAI-PMH envelope"),
        (status = 503, description = "The anonymous visibility index is unavailable; retryable. The body is a plain-text marker, not an OAI-PMH envelope")
    ),
    security(())
)]
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
    // Empty partial batches continue; only an exhausted initial window yields `noRecordsMatch`.
    // An emptied continuation closes with a terminal token to preserve the partial harvest.
    if page.entries.is_empty() && !page.more && params.resumption_token.is_none() {
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
            break;
        }
        body.push_str(&rendered);
        emitted += 1;
        last_cursor = Some(cursor.clone());
    }

    // Records held back here resume at the last emitted cursor; a fully emitted
    // page resumes past the last key the scan inspected.
    let cursor = if emitted < page.entries.len() {
        last_cursor
    } else if page.more {
        page.next_after.clone().or(last_cursor)
    } else {
        None
    };
    if let Some(cursor) = cursor {
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
    // Freeze an omitted upper bound so later records cannot enter an active sequence.
    let until_ms = until.map(|(ms, _)| ms).unwrap_or_else(current_time_ms);
    if from_ms > until_ms {
        return Err(protocol("badArgument", "from must not be after until"));
    }
    Ok((from_ms, until_ms, None))
}

fn current_time_ms() -> u64 {
    u64::try_from(chrono::Utc::now().timestamp_millis()).unwrap_or(0)
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
    let record = load_document_record(ctx.as_ref(), document_id)
        .await
        .map_err(|_| OaiFault::Internal)?
        // A record from another realm is not this repository's to serve, and a
        // public flag alone never crosses that boundary.
        .filter(|record| record.realm_id == realm_id && record.document_id == document_id);
    let Some(mut record) = record else {
        return Err(protocol("idDoesNotExist", "Unknown identifier"));
    };
    if !anon_can_read(ctx.as_ref(), &record).await? {
        return Err(protocol("idDoesNotExist", "Unknown identifier"));
    }
    record.updated_at_ms = effective_datestamp(ctx.as_ref(), &record)
        .await
        .map_err(visibility_fault)?;
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
    for (element, value) in jsonld_to_dc(&jsonld, &record.graph_iri) {
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
    let request = ExportMetadataRequest {
        document_id,
        auth: None,
        view: RoCrateExportView::Full,
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
        Ok(ExportMetadataResult::Full { jsonld, .. }) => Ok(jsonld),
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
#[path = "oai_tests.rs"]
mod tests;
