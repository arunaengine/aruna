//! Registration screening for tenant-supplied endpoints. The connect-time guard
//! in `aruna-blob` is what enforces the egress policy; this only makes a
//! spelling the http client reads differently fail here instead of at first use.

use url::Url;

/// Characters that end or move the authority. A bucket is spliced into it
/// (`//{bucket}.{host}`), so one of these inside a bucket picks a new host.
const AUTHORITY_BREAKS: [char; 5] = ['/', '\\', '?', '#', '@'];

/// Whether the http client reads the endpoint exactly as written. Its parser
/// turns `2852039166`, `0xa9fea9fe`, `127.1` and a trailing dot into addresses
/// the written host never shows.
pub fn is_canonical(endpoint: &str) -> bool {
    let Ok(url) = Url::parse(endpoint) else {
        return false;
    };
    url.host_str().is_some() && url.as_str().trim_end_matches('/') == endpoint.trim_end_matches('/')
}

pub fn breaks_authority(value: &str) -> bool {
    value.contains(AUTHORITY_BREAKS)
}

#[cfg(test)]
mod tests {
    use super::{breaks_authority, is_canonical};

    #[test]
    fn rejects_respelled_hosts() {
        // Every one of these parses into a link-local or loopback address.
        for host in [
            "2852039166",
            "0xa9fea9fe",
            "169.254.169.254.",
            "127.1",
            "2851995650",
            "0251.0376.0251.0376",
        ] {
            assert!(!is_canonical(&format!("https://{host}")), "{host}");
        }
    }

    #[test]
    fn accepts_plain_endpoints() {
        for endpoint in [
            "https://s3.example.com",
            "https://s3.example.com/",
            "https://minio.example.com:9000",
            "https://s3.example.com/prefix",
            "http://169.254.169.254",
        ] {
            assert!(is_canonical(endpoint), "{endpoint}");
        }
    }

    #[test]
    fn rejects_rewritten_endpoints() {
        // Case folding, punycode and a dropped default port all mean the client
        // would connect somewhere other than what an operator reads back.
        for endpoint in [
            "https://S3.Example.COM",
            "https://münchen.example",
            "https://s3.example.com:443",
            "https://good.example\\@169.254.169.254",
            "s3.example.com",
            "https://",
        ] {
            assert!(!is_canonical(endpoint), "{endpoint}");
        }
    }

    #[test]
    fn spots_authority_breaks() {
        for bucket in ["2852039166/", "a?x", "a#x", "a\\x", "a@x"] {
            assert!(breaks_authority(bucket), "{bucket}");
        }
        assert!(!breaks_authority("my.data-bucket"));
    }
}
