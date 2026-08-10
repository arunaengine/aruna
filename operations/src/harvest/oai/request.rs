use url::Url;

use aruna_core::structs::{HarvestGranularity, HarvestSelector};

/// Default OAI-PMH metadata schema when a source does not pin one.
pub const DEFAULT_METADATA_PREFIX: &str = "oai_dc";

/// Build a ListRecords request URL.
///
/// Per the OAI-PMH protocol, a `resumptionToken` is exclusive: when resuming, the
/// only arguments are `verb` and `resumptionToken`. A fresh request instead
/// carries `metadataPrefix` and the optional `set`/`from` window.
pub fn list_records_url(
    endpoint: &str,
    selector: &HarvestSelector,
    from: Option<&str>,
    resumption_token: Option<&str>,
) -> Result<Url, url::ParseError> {
    let mut url = Url::parse(endpoint)?;
    {
        let mut query = url.query_pairs_mut();
        query.clear();
        query.append_pair("verb", "ListRecords");
        if let Some(token) = resumption_token {
            query.append_pair("resumptionToken", token);
        } else {
            let prefix = selector
                .metadata_prefix
                .as_deref()
                .unwrap_or(DEFAULT_METADATA_PREFIX);
            query.append_pair("metadataPrefix", prefix);
            if let Some(set) = selector.set.as_deref() {
                query.append_pair("set", set);
            }
            if let Some(from) = from {
                query.append_pair("from", from);
            }
        }
    }
    Ok(url)
}

/// Build an Identify request URL.
pub fn identify_url(endpoint: &str) -> Result<Url, url::ParseError> {
    let mut url = Url::parse(endpoint)?;
    {
        let mut query = url.query_pairs_mut();
        query.clear();
        query.append_pair("verb", "Identify");
    }
    Ok(url)
}

/// Format Unix milliseconds as an OAI-PMH datestamp (UTC, second granularity).
pub fn format_from(datestamp_ms: u64) -> Option<String> {
    format_at(datestamp_ms, HarvestGranularity::Second)
}

/// Format a harvest cursor as an inclusive `from` bound at the provider's
/// advertised granularity. Re-fetched boundary records are rejected by
/// provenance staleness.
///
/// The epoch is not a harvest position but the absence of one: sending it would
/// make every first harvest carry a `from`, which a day-granularity repository
/// answers with `badArgument`.
pub fn format_window(datestamp_ms: u64, granularity: HarvestGranularity) -> Option<String> {
    if datestamp_ms == 0 {
        return None;
    }
    format_at(datestamp_ms, granularity)
}

fn format_at(datestamp_ms: u64, granularity: HarvestGranularity) -> Option<String> {
    let seconds = i64::try_from(datestamp_ms / 1000).ok()?;
    let instant = chrono::DateTime::from_timestamp(seconds, 0)?;
    Some(instant.format(granularity.format()).to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn selector() -> HarvestSelector {
        HarvestSelector {
            set: Some("alpha".to_string()),
            metadata_prefix: Some("oai_dc".to_string()),
        }
    }

    #[test]
    fn fresh_request_carries_prefix_set_and_from() {
        let url = list_records_url(
            "https://ex.org/oai",
            &selector(),
            Some("2026-01-01T00:00:00Z"),
            None,
        )
        .unwrap();
        let query = url.query().unwrap();
        assert!(query.contains("verb=ListRecords"));
        assert!(query.contains("metadataPrefix=oai_dc"));
        assert!(query.contains("set=alpha"));
        assert!(query.contains("from=2026-01-01"));
    }

    #[test]
    fn resumption_token_is_exclusive() {
        let url =
            list_records_url("https://ex.org/oai", &selector(), Some("x"), Some("TOK")).unwrap();
        let query = url.query().unwrap();
        assert!(query.contains("resumptionToken=TOK"));
        assert!(!query.contains("metadataPrefix"));
        assert!(!query.contains("set="));
        assert!(!query.contains("from="));
    }

    #[test]
    fn default_prefix_when_unset() {
        let url = list_records_url(
            "https://ex.org/oai",
            &HarvestSelector::default(),
            None,
            None,
        )
        .unwrap();
        assert!(url.query().unwrap().contains("metadataPrefix=oai_dc"));
    }

    #[test]
    fn window_formats_at_advertised_granularity() {
        assert_eq!(
            format_window(1000, HarvestGranularity::Second).as_deref(),
            Some("1970-01-01T00:00:01Z")
        );
        assert_eq!(
            format_window(1000, HarvestGranularity::Day).as_deref(),
            Some("1970-01-01")
        );
        assert_eq!(format_from(1000).as_deref(), Some("1970-01-01T00:00:01Z"));
    }

    #[test]
    fn epoch_cursor_sends_no_from() {
        for granularity in [HarvestGranularity::Day, HarvestGranularity::Second] {
            assert!(format_window(0, granularity).is_none());
        }
        let url = list_records_url("https://ex.org/oai", &selector(), None, None).unwrap();
        assert!(!url.query().unwrap().contains("from="));
    }

    #[test]
    fn identify_url_carries_only_the_verb() {
        let url = identify_url("https://ex.org/oai?stale=1").unwrap();
        assert_eq!(url.query(), Some("verb=Identify"));
    }
}
