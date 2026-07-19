//! Directory listing support for http staging sources.
//!
//! opendal's Http service only supports `stat` and `read`, so listing an
//! http connector walks classic autoindex pages (nginx, Apache, NCBI style)
//! instead: the directory URL is fetched and its anchor rows are parsed into
//! source entries. Responses that do not look like a directory index are
//! rejected with a clear error instead of being guessed at.

use aruna_core::errors::StagingSourceError;
use aruna_core::structs::{SourceEntry, SourceEntryKind};
use reqwest::StatusCode;
use reqwest::header::{ACCEPT, CONTENT_TYPE};
use std::collections::{HashMap, VecDeque};
use std::time::{Duration, SystemTime};

const MAX_INDEX_BYTES: usize = 16 * 1024 * 1024;
const MAX_INDEX_FETCHES: usize = 64;
const FETCH_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AutoindexEntry {
    pub name: String,
    pub is_dir: bool,
    pub size: Option<u64>,
    pub modified: Option<SystemTime>,
}

struct Anchor {
    href: String,
    text: String,
    trailing: String,
}

/// Lists one directory level (or a recursive walk) of an http staging source
/// by parsing autoindex pages, mirroring `list_operator` semantics for
/// limits, truncation, and the `files_only` filter.
pub(crate) async fn list_http_autoindex(
    config: &HashMap<String, String>,
    path: &str,
    limit: usize,
    recursive: bool,
    files_only: bool,
) -> Result<(Vec<SourceEntry>, bool), StagingSourceError> {
    let client = HttpIndexClient::from_config(config)?;
    let mut queue = VecDeque::from([path.trim_matches('/').to_string()]);
    let mut entries = Vec::new();
    let mut fetches = 0usize;

    while let Some(dir) = queue.pop_front() {
        if fetches == MAX_INDEX_FETCHES {
            return Ok((entries, true));
        }
        fetches += 1;
        let (base_path, html) = client.fetch_index(&dir).await?;
        let parsed = parse_autoindex(&base_path, &html).ok_or_else(|| {
            StagingSourceError::ListError(
                "http source response is not a recognizable directory index".to_string(),
            )
        })?;
        let prefix = if dir.is_empty() {
            String::new()
        } else {
            format!("{dir}/")
        };
        for entry in parsed {
            let child = format!("{prefix}{name}", name = entry.name);
            if entry.is_dir && recursive {
                queue.push_back(child.clone());
            }
            let kind = if entry.is_dir {
                if files_only {
                    continue;
                }
                SourceEntryKind::Directory
            } else {
                SourceEntryKind::File
            };
            if entries.len() == limit {
                return Ok((entries, true));
            }
            entries.push(SourceEntry {
                name: entry.name,
                path: child,
                kind,
                size: if kind == SourceEntryKind::File {
                    entry.size
                } else {
                    None
                },
                modified: entry.modified,
            });
        }
        if !recursive {
            break;
        }
    }

    Ok((entries, false))
}

struct HttpIndexClient {
    client: reqwest::Client,
    base: reqwest::Url,
    username: Option<String>,
    password: Option<String>,
    token: Option<String>,
}

impl HttpIndexClient {
    fn from_config(config: &HashMap<String, String>) -> Result<Self, StagingSourceError> {
        let endpoint = config.get("endpoint").ok_or_else(|| {
            StagingSourceError::OperatorCreationFailed(
                "http connector endpoint is missing".to_string(),
            )
        })?;
        let mut base = reqwest::Url::parse(endpoint).map_err(|error| {
            StagingSourceError::OperatorCreationFailed(format!("invalid http endpoint: {error}"))
        })?;
        if !base.path().ends_with('/') {
            let path = format!("{}/", base.path());
            base.set_path(&path);
        }
        if let Some(root) = config.get("root") {
            base = join_directory(&base, root)?;
        }
        let client = reqwest::Client::builder()
            .timeout(FETCH_TIMEOUT)
            .build()
            .map_err(|error| StagingSourceError::OperatorCreationFailed(error.to_string()))?;
        Ok(Self {
            client,
            base,
            username: config.get("username").cloned(),
            password: config.get("password").cloned(),
            token: config.get("token").cloned(),
        })
    }

    async fn fetch_index(&self, path: &str) -> Result<(String, String), StagingSourceError> {
        let url = join_directory(&self.base, path)?;
        let mut request = self.client.get(url).header(ACCEPT, "text/html");
        if let Some(token) = &self.token {
            request = request.bearer_auth(token);
        } else if let Some(username) = &self.username {
            request = request.basic_auth(username, self.password.as_deref());
        }
        let mut response = request.send().await.map_err(|error| {
            StagingSourceError::ListError(format!("http request failed: {error}"))
        })?;

        let status = response.status();
        if status == StatusCode::NOT_FOUND || status == StatusCode::GONE {
            return Err(StagingSourceError::NotFound);
        }
        if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
            return Err(StagingSourceError::AccessDenied);
        }
        if !status.is_success() {
            return Err(StagingSourceError::ListError(format!(
                "http source returned status {status}"
            )));
        }
        let content_type = response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_ascii_lowercase();
        if !content_type.starts_with("text/html") && !content_type.starts_with("application/xhtml")
        {
            return Err(StagingSourceError::ListError(format!(
                "http source returned `{}` instead of an HTML directory index",
                if content_type.is_empty() {
                    "an unknown content type"
                } else {
                    content_type.as_str()
                }
            )));
        }

        let mut base_path = response.url().path().to_string();
        if !base_path.ends_with('/') {
            base_path.push('/');
        }
        let mut body: Vec<u8> = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|error| {
            StagingSourceError::ListError(format!("http request failed: {error}"))
        })? {
            if body.len() + chunk.len() > MAX_INDEX_BYTES {
                return Err(StagingSourceError::ListError(
                    "directory index response exceeds the size limit".to_string(),
                ));
            }
            body.extend_from_slice(&chunk);
        }
        Ok((base_path, String::from_utf8_lossy(&body).into_owned()))
    }
}

fn join_directory(base: &reqwest::Url, path: &str) -> Result<reqwest::Url, StagingSourceError> {
    let segments = path
        .split('/')
        .filter(|segment| !segment.is_empty())
        .map(encode_segment)
        .collect::<Vec<_>>();
    if segments.is_empty() {
        return Ok(base.clone());
    }
    let relative = format!("./{}/", segments.join("/"));
    base.join(&relative)
        .map_err(|error| StagingSourceError::ListError(format!("invalid listing path: {error}")))
}

fn encode_segment(segment: &str) -> String {
    let mut encoded = String::with_capacity(segment.len());
    for byte in segment.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                encoded.push(byte as char);
            }
            _ => encoded.push_str(&format!("%{byte:02X}")),
        }
    }
    encoded
}

/// Parses an autoindex-style HTML document into directory entries.
///
/// `base_path` is the server-absolute URL path of the listed directory and is
/// used to resolve absolute-path hrefs. Returns `None` when the document does
/// not look like a directory index.
pub(crate) fn parse_autoindex(base_path: &str, html: &str) -> Option<Vec<AutoindexEntry>> {
    let lower = html.to_ascii_lowercase();
    let anchors = collect_anchors(html, &lower);
    let is_index = lower.contains("index of")
        || anchors.iter().any(|anchor| {
            anchor.href.trim() == "../"
                || anchor.text.trim().eq_ignore_ascii_case("parent directory")
        });
    if !is_index {
        return None;
    }

    let base_path = normalized_base_path(base_path);
    let mut entries: Vec<AutoindexEntry> = Vec::new();
    let mut index_by_name: HashMap<String, usize> = HashMap::new();
    for anchor in anchors {
        let Some((name, is_dir)) = entry_from_anchor(&base_path, &anchor) else {
            continue;
        };
        let (size, modified) = parse_trailing(&anchor.trailing);
        match index_by_name.get(&name) {
            // Fancy indexes link both an icon and the name to the same
            // target; merge duplicates instead of listing them twice.
            Some(&existing) => {
                let entry = &mut entries[existing];
                entry.size = entry.size.or(size);
                entry.modified = entry.modified.or(modified);
            }
            None => {
                index_by_name.insert(name.clone(), entries.len());
                entries.push(AutoindexEntry {
                    name,
                    is_dir,
                    size,
                    modified,
                });
            }
        }
    }
    Some(entries)
}

fn normalized_base_path(base_path: &str) -> String {
    let mut normalized = if base_path.starts_with('/') {
        base_path.to_string()
    } else {
        format!("/{base_path}")
    };
    if !normalized.ends_with('/') {
        normalized.push('/');
    }
    normalized
}

fn collect_anchors(html: &str, lower: &str) -> Vec<Anchor> {
    let mut starts = Vec::new();
    let mut pos = 0;
    while let Some(found) = lower[pos..].find("<a") {
        let start = pos + found;
        pos = start + 2;
        if lower
            .as_bytes()
            .get(start + 2)
            .is_some_and(u8::is_ascii_whitespace)
        {
            starts.push(start);
        }
    }

    let mut anchors = Vec::new();
    for (index, &start) in starts.iter().enumerate() {
        let Some(tag_end) = lower[start..].find('>').map(|offset| start + offset) else {
            continue;
        };
        let Some(href) = extract_href(&html[start..tag_end], &lower[start..tag_end]) else {
            continue;
        };
        let Some(text_end) = lower[tag_end..].find("</a").map(|offset| tag_end + offset) else {
            continue;
        };
        let text = strip_tags(&html[tag_end + 1..text_end]);
        let close = lower[text_end..]
            .find('>')
            .map(|offset| text_end + offset + 1)
            .unwrap_or(text_end);
        let next = starts.get(index + 1).copied().unwrap_or(html.len());
        let trailing = if close < next {
            strip_tags(&html[close..next])
        } else {
            String::new()
        };
        anchors.push(Anchor {
            href: decode_entities(href.trim()),
            text: decode_entities(text.trim()),
            trailing,
        });
    }
    anchors
}

fn extract_href<'tag>(tag: &'tag str, lower_tag: &str) -> Option<&'tag str> {
    let mut search = 0;
    loop {
        let found = lower_tag[search..].find("href")?;
        let start = search + found;
        search = start + 4;
        let before_ok = start == 0
            || lower_tag
                .as_bytes()
                .get(start - 1)
                .is_some_and(|byte| byte.is_ascii_whitespace());
        if !before_ok {
            continue;
        }
        let rest = tag[start + 4..].trim_start();
        let Some(rest) = rest.strip_prefix('=') else {
            continue;
        };
        let rest = rest.trim_start();
        return Some(match rest.as_bytes().first() {
            Some(&quote @ (b'"' | b'\'')) => {
                let value = &rest[1..];
                match value.find(quote as char) {
                    Some(end) => &value[..end],
                    None => value,
                }
            }
            _ => rest
                .split(|character: char| character.is_ascii_whitespace())
                .next()
                .unwrap_or(rest),
        });
    }
}

fn strip_tags(input: &str) -> String {
    let mut output = String::with_capacity(input.len());
    let mut in_tag = false;
    for character in input.chars() {
        match character {
            '<' => in_tag = true,
            '>' if in_tag => {
                in_tag = false;
                output.push(' ');
            }
            _ if !in_tag => output.push(character),
            _ => {}
        }
    }
    output
}

fn decode_entities(input: &str) -> String {
    let mut output = String::with_capacity(input.len());
    let mut rest = input;
    while let Some(position) = rest.find('&') {
        output.push_str(&rest[..position]);
        rest = &rest[position..];
        let Some(end) = rest
            .as_bytes()
            .iter()
            .take(12)
            .position(|&byte| byte == b';')
        else {
            output.push('&');
            rest = &rest[1..];
            continue;
        };
        let entity = &rest[1..end];
        let decoded = match entity {
            "amp" => Some('&'),
            "lt" => Some('<'),
            "gt" => Some('>'),
            "quot" => Some('"'),
            "apos" => Some('\''),
            "nbsp" => Some(' '),
            _ => entity
                .strip_prefix("#x")
                .or_else(|| entity.strip_prefix("#X"))
                .and_then(|hex| u32::from_str_radix(hex, 16).ok())
                .or_else(|| entity.strip_prefix('#').and_then(|dec| dec.parse().ok()))
                .and_then(char::from_u32),
        };
        match decoded {
            Some(character) => {
                output.push(character);
                rest = &rest[end + 1..];
            }
            None => {
                output.push('&');
                rest = &rest[1..];
            }
        }
    }
    output.push_str(rest);
    output
}

fn entry_from_anchor(base_path: &str, anchor: &Anchor) -> Option<(String, bool)> {
    let text = anchor.text.trim();
    if text.eq_ignore_ascii_case("parent directory") || text == ".." || text == "../" {
        return None;
    }

    let href = anchor.href.as_str();
    if href.is_empty() || href.starts_with('#') || href.starts_with('?') {
        return None;
    }
    // Full and protocol-relative URLs may point anywhere; skip them instead
    // of guessing whether they stay inside the listed directory.
    if href.starts_with("//") || href.contains("://") {
        return None;
    }
    let relative = if let Some(absolute) = href.strip_prefix('/') {
        // Server-absolute href: only direct children of the listed directory
        // are index rows.
        let full = format!("/{absolute}");
        full.strip_prefix(base_path)?.to_string()
    } else {
        let mut trimmed = href;
        while let Some(rest) = trimmed.strip_prefix("./") {
            trimmed = rest;
        }
        // A scheme prefix (mailto:, javascript:, ...) means this is not a
        // plain relative path.
        if trimmed
            .split('/')
            .next()
            .is_some_and(|head| head.contains(':'))
        {
            return None;
        }
        trimmed.to_string()
    };
    if relative.is_empty() || relative.contains('?') || relative.contains('#') {
        return None;
    }

    let is_dir = relative.ends_with('/');
    let segment = relative.trim_end_matches('/');
    if segment.is_empty() || segment.contains('/') {
        return None;
    }
    let name = percent_decode(segment)?;
    if name.is_empty()
        || name == "."
        || name == ".."
        || name.contains('/')
        || name.contains('\\')
        || name.chars().any(|character| character.is_control())
    {
        return None;
    }
    Some((name, is_dir))
}

fn percent_decode(input: &str) -> Option<String> {
    let bytes = input.as_bytes();
    let mut output = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            let high = (*bytes.get(index + 1)? as char).to_digit(16)?;
            let low = (*bytes.get(index + 2)? as char).to_digit(16)?;
            output.push((high * 16 + low) as u8);
            index += 3;
        } else {
            output.push(bytes[index]);
            index += 1;
        }
    }
    String::from_utf8(output).ok()
}

fn parse_trailing(trailing: &str) -> (Option<u64>, Option<SystemTime>) {
    let tokens = trailing.split_whitespace().collect::<Vec<_>>();
    for (index, token) in tokens.iter().enumerate() {
        let Some((year, month, day)) = parse_date_token(token) else {
            continue;
        };
        return match tokens
            .get(index + 1)
            .and_then(|time| parse_time_token(time))
        {
            Some((hour, minute, second)) => (
                tokens
                    .get(index + 2)
                    .and_then(|size| parse_size_token(size)),
                civil_to_system_time(year, month, day, hour, minute, second),
            ),
            None => (
                tokens
                    .get(index + 1)
                    .and_then(|size| parse_size_token(size)),
                civil_to_system_time(year, month, day, 0, 0, 0),
            ),
        };
    }
    (None, None)
}

fn parse_date_token(token: &str) -> Option<(i64, u32, u32)> {
    let bytes = token.as_bytes();
    if bytes.len() == 10 && bytes[4] == b'-' && bytes[7] == b'-' {
        // YYYY-MM-DD (Apache fancy index, NCBI).
        let year = token[..4].parse().ok()?;
        let month = token[5..7].parse().ok()?;
        let day = token[8..10].parse().ok()?;
        return validate_date(year, month, day);
    }
    if bytes.len() == 11 && bytes[2] == b'-' && bytes[6] == b'-' {
        // DD-MMM-YYYY (nginx autoindex).
        let day = token[..2].parse().ok()?;
        let month = month_from_abbreviation(&token[3..6])?;
        let year = token[7..11].parse().ok()?;
        return validate_date(year, month, day);
    }
    None
}

fn validate_date(year: i64, month: u32, day: u32) -> Option<(i64, u32, u32)> {
    ((1970..=9999).contains(&year) && (1..=12).contains(&month) && (1..=31).contains(&day))
        .then_some((year, month, day))
}

fn month_from_abbreviation(abbreviation: &str) -> Option<u32> {
    const MONTHS: [&str; 12] = [
        "jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec",
    ];
    let lower = abbreviation.to_ascii_lowercase();
    MONTHS
        .iter()
        .position(|month| *month == lower)
        .map(|index| index as u32 + 1)
}

fn parse_time_token(token: &str) -> Option<(u32, u32, u32)> {
    let mut parts = token.split(':');
    let hour = parts.next()?.parse().ok()?;
    let minute = parts.next()?.parse().ok()?;
    let second = parts.next().map_or(Some(0), |part| part.parse().ok())?;
    if parts.next().is_some() || hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    Some((hour, minute, second))
}

fn parse_size_token(token: &str) -> Option<u64> {
    if token == "-" {
        return None;
    }
    if token.bytes().all(|byte| byte.is_ascii_digit()) {
        return token.parse().ok();
    }
    let (number, suffix) = token.split_at(token.len().checked_sub(1)?);
    let multiplier: u64 = match suffix {
        "K" | "k" => 1024,
        "M" | "m" => 1024 * 1024,
        "G" | "g" => 1024 * 1024 * 1024,
        "T" | "t" => 1024_u64.pow(4),
        _ => return None,
    };
    let value: f64 = number.parse().ok()?;
    if !value.is_finite() || value < 0.0 {
        return None;
    }
    Some((value * multiplier as f64) as u64)
}

/// Converts a civil date and time to a `SystemTime` (days-from-civil
/// algorithm), avoiding a calendar dependency for best-effort timestamps.
fn civil_to_system_time(
    year: i64,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
    second: u32,
) -> Option<SystemTime> {
    let adjusted_year = if month <= 2 { year - 1 } else { year };
    let era = adjusted_year.div_euclid(400);
    let year_of_era = adjusted_year.rem_euclid(400);
    let month_prime = i64::from((month + 9) % 12);
    let day_of_year = (153 * month_prime + 2) / 5 + i64::from(day) - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    let days = era * 146_097 + day_of_era - 719_468;
    let seconds =
        days * 86_400 + i64::from(hour) * 3_600 + i64::from(minute) * 60 + i64::from(second);
    u64::try_from(seconds)
        .ok()
        .map(|seconds| SystemTime::UNIX_EPOCH + Duration::from_secs(seconds))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    const NGINX_FIXTURE: &str = include_str!("../fixtures/autoindex_nginx.html");
    const APACHE_PRE_FIXTURE: &str = include_str!("../fixtures/autoindex_apache_pre.html");
    const APACHE_TABLE_FIXTURE: &str = include_str!("../fixtures/autoindex_apache_table.html");
    const NON_INDEX_FIXTURE: &str = include_str!("../fixtures/non_index.html");

    fn epoch(seconds: u64) -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_secs(seconds)
    }

    #[test]
    fn parses_nginx_autoindex() {
        let entries = parse_autoindex("/download/", NGINX_FIXTURE).unwrap();

        assert_eq!(entries.len(), 4);
        assert_eq!(entries[0].name, "patches");
        assert!(entries[0].is_dir);
        assert_eq!(entries[0].size, None);
        assert_eq!(entries[1].name, "nginx-0.1.0.tar.gz");
        assert!(!entries[1].is_dir);
        assert_eq!(entries[1].size, Some(220_038));
        assert_eq!(entries[1].modified, Some(epoch(1_096_990_740)));
        assert_eq!(entries[3].name, "release notes.txt");
        assert_eq!(entries[3].size, Some(1_024));
    }

    #[test]
    fn parses_apache_pre_autoindex() {
        let entries = parse_autoindex("/refseq/release/", APACHE_PRE_FIXTURE).unwrap();

        // The parent link and the absolute footer URL are not index rows.
        let names = entries
            .iter()
            .map(|entry| entry.name.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            names,
            [
                "announcements",
                "complete",
                "viral",
                "README",
                "RELEASE_NUMBER"
            ]
        );
        assert!(entries[0].is_dir);
        assert_eq!(entries[0].modified, Some(epoch(1_783_601_880)));
        let readme = &entries[3];
        assert!(!readme.is_dir);
        assert_eq!(readme.size, Some(18 * 1024));
        let release_number = &entries[4];
        assert_eq!(release_number.size, Some(4));
    }

    #[test]
    fn parses_apache_table_autoindex() {
        let entries = parse_autoindex("/pub/data/", APACHE_TABLE_FIXTURE).unwrap();

        // Sort links (?C=N;O=D) and the parent row are skipped.
        let names = entries
            .iter()
            .map(|entry| entry.name.as_str())
            .collect::<Vec<_>>();
        assert_eq!(names, ["genomes", "checksums.txt", "reads&meta.tsv"]);
        assert!(entries[0].is_dir);
        assert_eq!(entries[0].modified, Some(epoch(1_783_606_800)));
        assert_eq!(entries[1].size, Some((2.5f64 * 1024.0) as u64));
        assert_eq!(entries[2].size, Some(731));
    }

    #[test]
    fn rejects_non_index_html() {
        assert_eq!(parse_autoindex("/", NON_INDEX_FIXTURE), None);
    }

    #[test]
    fn normalizes_hrefs_and_rejects_traversal() {
        let html = r##"<html><head><title>Index of /base/dir</title></head><body><pre>
<a href="../">../</a>
<a href="../evil.txt">../evil.txt</a>
<a href="%2e%2e/">dotdot</a>
<a href="/outside/path/">outside</a>
<a href="/base/dir/child2/">child2/</a>
<a href="/base/dir/deep/nested.txt">nested</a>
<a href="sub/dir.txt">nested relative</a>
<a href="?C=N;O=D">sort</a>
<a href="#section">fragment</a>
<a href="mailto:admin@example.org">mail</a>
<a href="https://other.example.org/file.txt">full url</a>
<a href="./child/">child/</a>
<a href="foo%20bar.txt">foo bar.txt</a>
</pre></body></html>"##;

        let entries = parse_autoindex("/base/dir/", html).unwrap();

        let names = entries
            .iter()
            .map(|entry| (entry.name.as_str(), entry.is_dir))
            .collect::<Vec<_>>();
        assert_eq!(
            names,
            [("child2", true), ("child", true), ("foo bar.txt", false)]
        );
    }

    async fn spawn_index_server(
        routes: Vec<(&'static str, &'static str, String)>,
    ) -> (tokio::task::JoinHandle<()>, String) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let routes = routes.clone();
                tokio::spawn(async move {
                    let mut buffer = vec![0u8; 4096];
                    let mut request = Vec::new();
                    loop {
                        let Ok(read) = socket.read(&mut buffer).await else {
                            return;
                        };
                        if read == 0 {
                            return;
                        }
                        request.extend_from_slice(&buffer[..read]);
                        if request.windows(4).any(|window| window == b"\r\n\r\n") {
                            break;
                        }
                    }
                    let request = String::from_utf8_lossy(&request).into_owned();
                    let path = request
                        .split_whitespace()
                        .nth(1)
                        .unwrap_or_default()
                        .to_string();
                    let response = match routes
                        .iter()
                        .find(|(route_path, ..)| *route_path == path)
                    {
                        Some((_, content_type, body)) => format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                            body.len()
                        ),
                        None =>
                            "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                                .to_string(),
                    };
                    let _ = socket.write_all(response.as_bytes()).await;
                    let _ = socket.shutdown().await;
                });
            }
        });
        (server, format!("http://{address}"))
    }

    fn index_page(title_path: &str, rows: &str) -> String {
        format!(
            "<html><head><title>Index of {title_path}</title></head><body><h1>Index of {title_path}</h1><hr><pre><a href=\"../\">../</a>\n{rows}</pre><hr></body></html>"
        )
    }

    #[tokio::test]
    async fn lists_http_autoindex_with_limit() {
        let rows = concat!(
            "<a href=\"sub/\">sub/</a>                 19-Jul-2026 10:00       -\n",
            "<a href=\"a.txt\">a.txt</a>               19-Jul-2026 10:01       10\n",
            "<a href=\"b.txt\">b.txt</a>               19-Jul-2026 10:02       20\n",
        );
        let (server, endpoint) = spawn_index_server(vec![(
            "/data/",
            "text/html; charset=utf-8",
            index_page("/data/", rows),
        )])
        .await;
        let config = HashMap::from([
            ("endpoint".to_string(), endpoint),
            ("root".to_string(), "data".to_string()),
        ]);

        let (entries, truncated) = list_http_autoindex(&config, "", 2, false, false)
            .await
            .unwrap();

        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].name, "sub");
        assert_eq!(entries[0].kind, SourceEntryKind::Directory);
        assert_eq!(entries[1].name, "a.txt");
        assert_eq!(entries[1].kind, SourceEntryKind::File);
        assert_eq!(entries[1].size, Some(10));
        assert!(truncated);

        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn lists_http_autoindex_recursively() {
        let root_rows = concat!(
            "<a href=\"sub/\">sub/</a>                 19-Jul-2026 10:00       -\n",
            "<a href=\"a.txt\">a.txt</a>               19-Jul-2026 10:01       10\n",
        );
        let sub_rows = "<a href=\"nested.txt\">nested.txt</a>      19-Jul-2026 10:03       30\n";
        let (server, endpoint) = spawn_index_server(vec![
            ("/", "text/html", index_page("/", root_rows)),
            ("/sub/", "text/html", index_page("/sub/", sub_rows)),
        ])
        .await;
        let config = HashMap::from([("endpoint".to_string(), endpoint)]);

        let (entries, truncated) = list_http_autoindex(&config, "", 10, true, true)
            .await
            .unwrap();

        let paths = entries
            .iter()
            .map(|entry| entry.path.as_str())
            .collect::<Vec<_>>();
        assert_eq!(paths, ["a.txt", "sub/nested.txt"]);
        assert!(!truncated);

        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn rejects_non_html_listing_response() {
        let (server, endpoint) = spawn_index_server(vec![(
            "/",
            "application/json",
            "{\"not\":\"an index\"}".to_string(),
        )])
        .await;
        let config = HashMap::from([("endpoint".to_string(), endpoint)]);

        let error = list_http_autoindex(&config, "", 10, false, false)
            .await
            .unwrap_err();
        assert!(matches!(error, StagingSourceError::ListError(_)));

        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn maps_missing_directory_to_not_found() {
        let (server, endpoint) = spawn_index_server(Vec::new()).await;
        let config = HashMap::from([("endpoint".to_string(), endpoint)]);

        let error = list_http_autoindex(&config, "missing", 10, false, false)
            .await
            .unwrap_err();
        assert_eq!(error, StagingSourceError::NotFound);

        server.abort();
        let _ = server.await;
    }
}
