//! A plain HTML index for public folders. Only an unsigned request that asks for
//! HTML and names a folder key gets it; SDK, CLI and signed requests keep S3 semantics.

use http::HeaderMap;
use http::header::ACCEPT;
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use s3s::dto::{Timestamp, TimestampFormat};

/// Link segments keep RFC 3986 unreserved characters and encode everything else.
const SEGMENT: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~');

/// One row of the index: a subfolder or an object directly below the folder.
pub(crate) struct IndexEntry {
    /// The name below the listed folder; a subfolder keeps its trailing `/`.
    pub name: String,
    pub size: Option<i64>,
    pub modified: Option<Timestamp>,
}

/// True when the request names a folder key and its `Accept` header lists
/// `text/html`, as browser navigation does and S3 clients do not.
pub(crate) fn wants_index(headers: &HeaderMap, key: &str) -> bool {
    key.ends_with('/')
        && headers
            .get_all(ACCEPT)
            .iter()
            .filter_map(|value| value.to_str().ok())
            .flat_map(|value| value.split(','))
            .filter_map(|range| range.split(';').next())
            .any(|media| media.trim().eq_ignore_ascii_case("text/html"))
}

/// Renders the folder index with escaped names and relative, encoded links.
pub(crate) fn render_index(
    bucket: &str,
    prefix: &str,
    entries: &[IndexEntry],
    truncated: bool,
) -> String {
    let title = escape(&format!("Index of /{bucket}/{prefix}"));
    let mut rows = String::new();
    if prefix.trim_end_matches('/').contains('/') {
        rows.push_str("<tr><td><a href=\"../\">../</a></td><td></td><td></td></tr>\n");
    }
    for entry in entries {
        let href = match entry.name.strip_suffix('/') {
            Some(folder) => format!("{}/", utf8_percent_encode(folder, SEGMENT)),
            None => utf8_percent_encode(&entry.name, SEGMENT).to_string(),
        };
        let modified = entry
            .modified
            .as_ref()
            .and_then(http_date)
            .unwrap_or_default();
        let size = entry.size.map(|size| size.to_string()).unwrap_or_default();
        rows.push_str(&format!(
            "<tr><td><a href=\"{}\">{}</a></td><td>{}</td><td class=\"size\">{}</td></tr>\n",
            escape(&href),
            escape(&entry.name),
            escape(&modified),
            size,
        ));
    }
    let note = if truncated {
        "<p>Only the first page of entries is shown.</p>\n"
    } else {
        ""
    };
    format!(
        "<!doctype html>\n<html lang=\"en\"><head><meta charset=\"utf-8\"><title>{title}</title>\n\
<style>body{{font-family:sans-serif;margin:2rem}}td{{padding:.2rem 1.5rem .2rem 0}}\
.size{{text-align:right}}</style></head>\n<body><h1>{title}</h1>\n\
<table><thead><tr><th align=\"left\">Name</th><th align=\"left\">Last modified</th>\
<th align=\"right\">Size</th></tr></thead><tbody>\n{rows}</tbody></table>\n{note}</body></html>\n"
    )
}

fn http_date(timestamp: &Timestamp) -> Option<String> {
    let mut buffer = Vec::new();
    timestamp
        .format(TimestampFormat::HttpDate, &mut buffer)
        .ok()?;
    String::from_utf8(buffer).ok()
}

fn escape(text: &str) -> String {
    let mut escaped = String::with_capacity(text.len());
    for character in text.chars() {
        match character {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '"' => escaped.push_str("&quot;"),
            '\'' => escaped.push_str("&#39;"),
            other => escaped.push(other),
        }
    }
    escaped
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderValue;

    fn accept(value: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(ACCEPT, HeaderValue::from_str(value).unwrap());
        headers
    }

    #[test]
    fn browser_wants_index() {
        let browser = accept("text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8");
        assert!(wants_index(&browser, "test/"));
        assert!(wants_index(&accept("TEXT/HTML; q=1"), "a/b/"));
    }

    #[test]
    fn clients_keep_objects() {
        assert!(!wants_index(&HeaderMap::new(), "test/"));
        assert!(!wants_index(&accept("*/*"), "test/"));
        assert!(!wants_index(&accept("application/xml"), "test/"));
        let browser = accept("text/html,*/*");
        assert!(!wants_index(&browser, "test/readme.txt"));
        assert!(!wants_index(&accept("text/htmlx"), "test/"));
    }

    #[test]
    fn index_escapes_names() {
        let entries = [
            IndexEntry {
                name: "sub dir/".into(),
                size: None,
                modified: None,
            },
            IndexEntry {
                name: "<script>&\"x\".txt".into(),
                size: Some(12),
                modified: None,
            },
        ];
        let html = render_index("bucket", "top/", &entries, false);

        assert!(html.contains("<a href=\"sub%20dir/\">sub dir/</a>"));
        assert!(html.contains("&lt;script&gt;&amp;&quot;x&quot;.txt"));
        assert!(!html.contains("<script>"));
        assert!(html.contains("<td class=\"size\">12</td>"));
        assert!(!html.contains("href=\"../\""));
        assert!(!html.contains("first page"));
    }

    #[test]
    fn nested_index_parent() {
        let html = render_index("bucket", "top/inner/", &[], true);

        assert!(html.contains("<a href=\"../\">../</a>"));
        assert!(html.contains("Index of /bucket/top/inner/"));
        assert!(html.contains("Only the first page of entries is shown."));
    }
}
