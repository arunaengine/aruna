use std::collections::HashSet;

use anyhow::Result;
use http::Method;
use rand::distr::SampleString as DistString;
use reqsign::{AwsCredential, AwsV4Signer};
use url::Url;

#[tracing::instrument(
    level = "trace",
    skip(
        method,
        access_key,
        secret_key,
        ssl,
        multipart,
        part_number,
        upload_id,
        bucket,
        key,
        endpoint,
        duration
    )
)]
/// Creates a fully customized presigned S3 url.
///
/// ## Arguments:
///
/// * `method: http::Method` - Http method the request is valid for
/// * `access_key: &String` - Secret key id
/// * `secret_key: &String` - Secret key for access
/// * `ssl: bool` - Flag if the endpoint is accessible via ssl
/// * `multipart: bool` - Flag if the request is for a specific multipart part upload
/// * `part_number: i32` - Specific part number if multipart: true
/// * `upload_id: &String` - Multipart upload id if multipart: true
/// * `bucket: &String` - Bucket name
/// * `key: &String` - Full path of object in bucket
/// * `endpoint: &String` - Full path of object in bucket
/// * `duration: i64` - Full path of object in bucket
/// *
///
/// ## Returns:
///
/// * `` -
///
#[allow(clippy::too_many_arguments)]
pub fn sign_url(
    method: reqwest::Method,
    access_key: &str,
    secret_key: &str,
    ssl: bool,
    multipart: bool,
    part_number: i32,
    upload_id: &str,
    bucket: &str,
    key: &str,
    endpoint: &str,
    duration: i64,
) -> Result<String> {
    let signer = AwsV4Signer::new("s3", "RegionOne");

    // Set protocol depending if ssl
    let protocol = if ssl { "https://" } else { "http://" };

    // Remove http:// or https:// from beginning of endpoint url if present
    let endpoint_sanitized = if let Some(stripped) = endpoint.strip_prefix("https://") {
        stripped.to_string()
    } else if let Some(stripped) = endpoint.strip_prefix("http://") {
        stripped.to_string()
    } else {
        endpoint.to_string()
    };

    // Construct request
    let url = if multipart {
        Url::parse(&format!(
            "{protocol}{bucket}.{endpoint_sanitized}/{key}?partNumber={part_number}&uploadId={upload_id}"
        ))
        .inspect_err(|&e| {
            tracing::error!(error = ?e, msg = e.to_string());
        })?
    } else {
        Url::parse(&format!("{protocol}{bucket}.{endpoint_sanitized}/{key}")).inspect_err(|&e| {
            tracing::error!(error = ?e, msg = e.to_string());
        })?
    };

    let mut req = reqwest::Request::new(method, url);

    // Signing request with Signer
    signer
        .sign_query(
            &mut req,
            std::time::Duration::new(duration as u64, 0), // Sec, nano
            &AwsCredential {
                access_key_id: access_key.to_string(),
                secret_access_key: secret_key.to_string(),
                session_token: None,
                expires_in: None,
            },
        )
        .map_err(|e| {
            tracing::error!(error = ?e, msg = e.to_string());
            e
        })?;
    Ok(req.url().to_string())
}

#[tracing::instrument(
    level = "trace",
    skip(access_key, secret_key, ssl, bucket, key, endpoint)
)]
/// Convenience wrapper function for sign_url(...) to reduce unused parameters for download url.
pub fn sign_download_url(
    access_key: &str,
    secret_key: &str,
    ssl: bool,
    bucket: &str,
    key: &str,
    endpoint: &str,
) -> Result<String> {
    sign_url(
        reqwest::Method::GET,
        access_key,
        secret_key,
        ssl,
        false,
        0,
        "",
        bucket,
        key,
        endpoint,
        604800, //Note: Default 1 week until requests allow custom duration
    )
}

pub fn is_method_read(method: &Method) -> bool {
    matches!(method, &Method::GET | &Method::HEAD | &Method::OPTIONS)
}

pub fn random_string(len: usize) -> String {
    use rand::distr::Alphanumeric;
    use rand::rng as thread_rng;
    Alphanumeric
        .sample_string(&mut thread_rng(), len)
        .to_ascii_lowercase()
}

pub fn bucket_path_from_pathstring(path: &str) -> Result<(String, String)> {
    if path.starts_with("s3://") {
        let parts = path[5..].split_once('/');
        if let Some((bucket, key)) = parts {
            Ok((bucket.to_string(), key.to_string()))
        } else {
            Err(anyhow::anyhow!("Invalid path format"))
        }
    } else if path.starts_with("file://") {
        let parts = path[7..].split_once('/');
        if let Some((bucket, key)) = parts {
            Ok((bucket.to_string(), key.to_string()))
        } else {
            Err(anyhow::anyhow!("Invalid path format"))
        }
    } else {
        Err(anyhow::anyhow!("Invalid path format"))
    }
}

pub trait IntoOption {
    fn into_option(self) -> Option<Self>
    where
        Self: Sized;
}

impl<T, S> IntoOption for HashSet<T, S> {
    fn into_option(self) -> Option<Self> {
        if self.is_empty() {
            None
        } else {
            Some(self)
        }
    }
}
