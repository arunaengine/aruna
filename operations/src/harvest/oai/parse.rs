use aruna_core::structs::HarvestGranularity;
use quick_xml::Reader;
use quick_xml::events::{BytesStart, Event};
use thiserror::Error;

/// OAI-PMH record header. `datestamp` stays the raw upstream string; callers
/// convert it with [`parse_datestamp_ms`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OaiHeader {
    pub identifier: String,
    pub datestamp: String,
    pub deleted: bool,
    pub sets: Vec<String>,
}

/// One harvested record. `dc` holds the `oai_dc` elements as (local name, value)
/// pairs, preserving repeats and order; empty for a deleted record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OaiRecord {
    pub header: OaiHeader,
    pub dc: Vec<(String, String)>,
}

/// A ListRecords/ListIdentifiers page: its records and the resumption token to
/// continue with, if any.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OaiPage {
    pub records: Vec<OaiRecord>,
    pub resumption_token: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Error)]
pub enum OaiParseError {
    #[error("malformed OAI-PMH XML: {0}")]
    Xml(String),
    #[error("OAI-PMH error [{code}]: {message}")]
    Protocol { code: String, message: String },
}

/// Parse a ListRecords, ListIdentifiers, or GetRecord response.
///
/// An `<error code="noRecordsMatch">` is a valid empty page, not an error, so an
/// incremental harvest that finds nothing succeeds instead of failing.
pub fn parse_list_page(xml: &str) -> Result<OaiPage, OaiParseError> {
    // Keep raw text: a value split around an entity must not lose its spaces.
    // commit_text trims the fully reassembled value instead.
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(false);

    let mut records: Vec<OaiRecord> = Vec::new();
    let mut resumption_token: Option<String> = None;
    let mut error_code: Option<String> = None;
    let mut error_message = String::new();

    let mut stack: Vec<Vec<u8>> = Vec::new();
    let mut record: Option<OaiRecord> = None;
    // One element's text can arrive as several Text/GeneralRef events (quick-xml
    // splits around entities), so accumulate and commit at the closing tag.
    let mut text = String::new();

    loop {
        match reader.read_event() {
            Ok(Event::Start(element)) => {
                stack.push(local_name(&element));
                on_open(&stack, &element, &mut record, &mut error_code)?;
                text.clear();
            }
            Ok(Event::Empty(element)) => {
                stack.push(local_name(&element));
                on_open(&stack, &element, &mut record, &mut error_code)?;
                stack.pop();
            }
            Ok(Event::Text(chunk)) => {
                let raw = std::str::from_utf8(chunk.as_ref())
                    .map_err(|error| OaiParseError::Xml(error.to_string()))?;
                text.push_str(
                    quick_xml::escape::unescape(raw)
                        .map_err(|error| OaiParseError::Xml(error.to_string()))?
                        .as_ref(),
                );
            }
            // CDATA is literal by definition: append it verbatim, no unescaping.
            Ok(Event::CData(chunk)) => {
                text.push_str(
                    std::str::from_utf8(chunk.as_ref())
                        .map_err(|error| OaiParseError::Xml(error.to_string()))?,
                );
            }
            Ok(Event::GeneralRef(reference)) => {
                let name = std::str::from_utf8(reference.as_ref())
                    .map_err(|error| OaiParseError::Xml(error.to_string()))?
                    .trim_matches(|c| c == '&' || c == ';');
                let resolved = quick_xml::escape::unescape(&format!("&{name};"))
                    .map_err(|error| OaiParseError::Xml(error.to_string()))?
                    .into_owned();
                text.push_str(&resolved);
            }
            Ok(Event::End(_)) => {
                let slot = slot_of(&stack);
                commit_text(
                    slot,
                    stack.last().map(Vec::as_slice).unwrap_or_default(),
                    &text,
                    &mut record,
                    &mut resumption_token,
                    &mut error_message,
                );
                text.clear();
                if slot == Slot::Record
                    && let Some(done) = record.take()
                {
                    records.push(done);
                }
                stack.pop();
            }
            Ok(Event::Eof) => break,
            Err(error) => return Err(OaiParseError::Xml(error.to_string())),
            _ => {}
        }
    }

    if let Some(code) = error_code
        && code != "noRecordsMatch"
    {
        return Err(OaiParseError::Protocol {
            code,
            message: error_message,
        });
    }

    Ok(OaiPage {
        records,
        resumption_token: resumption_token.filter(|token| !token.is_empty()),
    })
}

/// The one place in the OAI-PMH envelope an element carries protocol meaning.
/// Everything else, including a payload element that reuses a protocol name
/// further down, is [`Slot::Payload`] and stays inert.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Slot {
    Error,
    Record,
    Header,
    ResumptionToken,
    Identifier,
    Datestamp,
    SetSpec,
    DcField,
    Payload,
}

/// Classify the element on top of `stack`, which holds the path from the
/// response root down to that element. Position is what decides, never the
/// local name alone: `OAI-PMH / verb / record / {header,metadata} / ...`.
fn slot_of(stack: &[Vec<u8>]) -> Slot {
    let Some(name) = stack.last().map(Vec::as_slice) else {
        return Slot::Payload;
    };
    let ancestor = |index: usize| stack.get(index).map(Vec::as_slice);
    match stack.len() {
        2 if name == b"error" => Slot::Error,
        3 if name == b"record" => Slot::Record,
        3 if name == b"resumptionToken" => Slot::ResumptionToken,
        4 if name == b"header" && ancestor(2) == Some(b"record".as_slice()) => Slot::Header,
        5 if ancestor(2) == Some(b"record".as_slice())
            && ancestor(3) == Some(b"header".as_slice()) =>
        {
            match name {
                b"identifier" => Slot::Identifier,
                b"datestamp" => Slot::Datestamp,
                b"setSpec" => Slot::SetSpec,
                _ => Slot::Payload,
            }
        }
        6 if ancestor(2) == Some(b"record".as_slice())
            && ancestor(3) == Some(b"metadata".as_slice())
            && ancestor(4) == Some(b"dc".as_slice()) =>
        {
            Slot::DcField
        }
        _ => Slot::Payload,
    }
}

fn on_open(
    stack: &[Vec<u8>],
    element: &BytesStart<'_>,
    record: &mut Option<OaiRecord>,
    error_code: &mut Option<String>,
) -> Result<(), OaiParseError> {
    match slot_of(stack) {
        Slot::Record => {
            *record = Some(OaiRecord {
                header: OaiHeader {
                    identifier: String::new(),
                    datestamp: String::new(),
                    deleted: false,
                    sets: Vec::new(),
                },
                dc: Vec::new(),
            });
        }
        Slot::Header => {
            if let Some(record) = record.as_mut() {
                record.header.deleted =
                    attribute(element, b"status")?.is_some_and(|status| status == "deleted");
            }
        }
        Slot::Error => {
            *error_code = attribute(element, b"code")?.or_else(|| Some("unknown".to_string()));
        }
        _ => {}
    }
    Ok(())
}

/// Route one element's accumulated text at its closing tag, by the slot the
/// element occupies. `name` is its local name, which only a Dublin Core field
/// needs, as the key it is stored under.
fn commit_text(
    slot: Slot,
    name: &[u8],
    value: &str,
    record: &mut Option<OaiRecord>,
    resumption_token: &mut Option<String>,
    error_message: &mut String,
) {
    let value = value.trim();
    match slot {
        Slot::Error => {
            *error_message = value.to_string();
            return;
        }
        Slot::ResumptionToken => {
            *resumption_token = Some(value.to_string());
            return;
        }
        _ => {}
    }

    let Some(record) = record.as_mut() else {
        return;
    };
    match slot {
        Slot::Identifier => record.header.identifier = value.to_string(),
        Slot::Datestamp => record.header.datestamp = value.to_string(),
        Slot::SetSpec if !value.is_empty() => record.header.sets.push(value.to_string()),
        Slot::DcField if !value.is_empty() => {
            record.dc.push((
                String::from_utf8_lossy(name).into_owned(),
                value.to_string(),
            ));
        }
        _ => {}
    }
}

fn local_name(element: &BytesStart<'_>) -> Vec<u8> {
    element.local_name().as_ref().to_vec()
}

fn attribute(element: &BytesStart<'_>, key: &[u8]) -> Result<Option<String>, OaiParseError> {
    for attribute in element.attributes() {
        let attribute = attribute.map_err(|error| OaiParseError::Xml(error.to_string()))?;
        if attribute.key.local_name().as_ref() == key {
            let value = attribute
                .unescape_value()
                .map_err(|error| OaiParseError::Xml(error.to_string()))?
                .into_owned();
            return Ok(Some(value));
        }
    }
    Ok(None)
}

/// Read the advertised granularity out of an `Identify` response. An
/// unrecognized or missing value leaves the caller on the baseline rather than
/// failing the harvest, since the verb is only a hint.
pub fn parse_granularity(xml: &str) -> Option<HarvestGranularity> {
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);
    let mut stack: Vec<Vec<u8>> = Vec::new();
    loop {
        match reader.read_event() {
            Ok(Event::Start(element)) => stack.push(local_name(&element)),
            Ok(Event::End(_)) => {
                stack.pop();
            }
            // `OAI-PMH / Identify / granularity`, never a repeat of the name
            // inside a repository description block.
            Ok(Event::Text(chunk)) if is_granularity(&stack) => {
                let raw = std::str::from_utf8(chunk.as_ref()).ok()?;
                return HarvestGranularity::parse(raw);
            }
            Ok(Event::Eof) | Err(_) => return None,
            _ => {}
        }
    }
}

fn is_granularity(stack: &[Vec<u8>]) -> bool {
    stack.len() == 3 && stack[1].as_slice() == b"Identify" && stack[2].as_slice() == b"granularity"
}

/// Convert an OAI-PMH datestamp (`YYYY-MM-DD` or RFC 3339) to Unix milliseconds.
pub fn parse_datestamp_ms(datestamp: &str) -> Option<u64> {
    if let Ok(instant) = chrono::DateTime::parse_from_rfc3339(datestamp) {
        return u64::try_from(instant.timestamp_millis()).ok();
    }
    let date = chrono::NaiveDate::parse_from_str(datestamp, "%Y-%m-%d").ok()?;
    let midnight = date.and_hms_opt(0, 0, 0)?;
    u64::try_from(midnight.and_utc().timestamp_millis()).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    const LIST: &str = r#"<?xml version="1.0"?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
  <ListRecords>
    <record>
      <header>
        <identifier>oai:example.org:1</identifier>
        <datestamp>2026-01-02T03:04:05Z</datestamp>
        <setSpec>alpha</setSpec>
      </header>
      <metadata>
        <oai_dc:dc xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/"
                   xmlns:dc="http://purl.org/dc/elements/1.1/">
          <dc:title>First &amp; only</dc:title>
          <dc:creator>Alice</dc:creator>
          <dc:creator>Bob</dc:creator>
        </oai_dc:dc>
      </metadata>
    </record>
    <record>
      <header status="deleted">
        <identifier>oai:example.org:2</identifier>
        <datestamp>2026-01-03</datestamp>
      </header>
    </record>
    <resumptionToken cursor="0">TOKEN-2</resumptionToken>
  </ListRecords>
</OAI-PMH>"#;

    #[test]
    fn parses_records_sets_deletions_and_token() {
        let page = parse_list_page(LIST).unwrap();
        assert_eq!(page.records.len(), 2);
        assert_eq!(page.resumption_token.as_deref(), Some("TOKEN-2"));

        let first = &page.records[0];
        assert_eq!(first.header.identifier, "oai:example.org:1");
        assert!(!first.header.deleted);
        assert_eq!(first.header.sets, vec!["alpha".to_string()]);
        assert_eq!(
            first.dc[0],
            ("title".to_string(), "First & only".to_string())
        );
        assert_eq!(first.dc.iter().filter(|(k, _)| k == "creator").count(), 2);

        let second = &page.records[1];
        assert!(second.header.deleted);
        assert!(second.dc.is_empty());
    }

    #[test]
    fn empty_element_resumption_token_ends_list() {
        let xml = LIST.replace(
            "<resumptionToken cursor=\"0\">TOKEN-2</resumptionToken>",
            "<resumptionToken/>",
        );
        let page = parse_list_page(&xml).unwrap();
        assert!(page.resumption_token.is_none());
    }

    #[test]
    fn no_records_match_is_empty_not_error() {
        let xml = r#"<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
          <error code="noRecordsMatch">no records</error></OAI-PMH>"#;
        let page = parse_list_page(xml).unwrap();
        assert!(page.records.is_empty());
        assert!(page.resumption_token.is_none());
    }

    #[test]
    fn protocol_error_is_reported() {
        let xml = r#"<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
          <error code="badArgument">bad</error></OAI-PMH>"#;
        assert_eq!(
            parse_list_page(xml).unwrap_err(),
            OaiParseError::Protocol {
                code: "badArgument".to_string(),
                message: "bad".to_string(),
            }
        );
    }

    /// Protocol names inside a record's payload must not steer the parse: an
    /// `error` there would fail the whole source permanently, and a `record`
    /// would discard the enclosing record.
    #[test]
    fn nested_names_inert() {
        let xml = LIST.replace(
            "<dc:creator>Bob</dc:creator>",
            r#"<dc:creator>Bob</dc:creator>
          <dc:description><error code="badArgument">nope</error>
            <record><header status="deleted"><identifier>oai:evil:1</identifier>
              <datestamp>2000-01-01</datestamp></header></record>
          </dc:description>"#,
        );
        let page = parse_list_page(&xml).unwrap();
        assert_eq!(page.records.len(), 2);
        assert_eq!(page.resumption_token.as_deref(), Some("TOKEN-2"));

        let first = &page.records[0];
        assert_eq!(first.header.identifier, "oai:example.org:1");
        assert!(!first.header.deleted);
        assert_eq!(first.header.sets, vec!["alpha".to_string()]);
        assert_eq!(
            first.dc[0],
            ("title".to_string(), "First & only".to_string())
        );
        assert_eq!(first.dc.iter().filter(|(k, _)| k == "creator").count(), 2);
    }

    #[test]
    fn nested_token_ignored() {
        let xml = LIST.replace(
            "<dc:creator>Bob</dc:creator>",
            "<dc:creator>Bob</dc:creator>
          <dc:relation><resumptionToken>TOKEN-EVIL</resumptionToken></dc:relation>",
        );
        let page = parse_list_page(&xml).unwrap();
        assert_eq!(page.resumption_token.as_deref(), Some("TOKEN-2"));
    }

    #[test]
    fn nested_granularity_ignored() {
        let identify = r#"<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/"><Identify>
            <description><oai-identifier><granularity>YYYY-MM-DD</granularity></oai-identifier>
            </description>
            <granularity>YYYY-MM-DDThh:mm:ssZ</granularity></Identify></OAI-PMH>"#;
        assert_eq!(
            parse_granularity(identify),
            Some(HarvestGranularity::Second)
        );
    }

    #[test]
    fn cdata_values_are_preserved() {
        let xml = LIST.replace(
            "<dc:title>First &amp; only</dc:title>",
            "<dc:title><![CDATA[Raw <b>&amp; markup</b>]]></dc:title>",
        );
        let page = parse_list_page(&xml).unwrap();
        assert_eq!(
            page.records[0].dc[0],
            ("title".to_string(), "Raw <b>&amp; markup</b>".to_string())
        );
    }

    #[test]
    fn mixed_text_and_cdata_concatenates() {
        let xml = LIST.replace(
            "<dc:title>First &amp; only</dc:title>",
            "<dc:title>a &amp; <![CDATA[b & c]]> d\u{e9}</dc:title>",
        );
        let page = parse_list_page(&xml).unwrap();
        assert_eq!(
            page.records[0].dc[0],
            ("title".to_string(), "a & b & c d\u{e9}".to_string())
        );
    }

    #[test]
    fn identify_granularity_is_read() {
        let identify = |value: &str| {
            format!(
                r#"<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/"><Identify>
                <repositoryName>Ex</repositoryName>
                <granularity>{value}</granularity></Identify></OAI-PMH>"#
            )
        };
        assert_eq!(
            parse_granularity(&identify("YYYY-MM-DD")),
            Some(HarvestGranularity::Day)
        );
        assert_eq!(
            parse_granularity(&identify("YYYY-MM-DDThh:mm:ssZ")),
            Some(HarvestGranularity::Second)
        );
        assert_eq!(parse_granularity(&identify("nonsense")), None);
        assert_eq!(parse_granularity("<OAI-PMH/>"), None);
    }

    #[test]
    fn datestamp_parses_both_forms() {
        assert_eq!(parse_datestamp_ms("1970-01-01"), Some(0));
        assert_eq!(parse_datestamp_ms("1970-01-01T00:00:01Z"), Some(1000));
        assert!(parse_datestamp_ms("not-a-date").is_none());
    }
}
