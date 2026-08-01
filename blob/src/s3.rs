use aruna_core::errors::BlobError;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region, RequestChecksumCalculation};
use aws_sdk_s3::types::{BucketLocationConstraint, CreateBucketConfiguration};
use std::collections::HashMap;

const DEFAULT_REGION: &str = "eu-central-1";
// AWS rejects CreateBucket requests that name us-east-1 explicitly.
const IMPLICIT_REGION: &str = "us-east-1";

pub async fn create_s3_client(
    endpoint: &str,
    region: Option<String>,
    access_key_id: &str,
    secret_key: &str,
    force_path_style: bool,
) -> Result<Client, BlobError> {
    let creds = Credentials::new(access_key_id, secret_key, None, None, "Aruna_v3");
    // An unpinned region makes the SDK resolve one through the EC2 metadata service.
    let region = Region::new(region.unwrap_or_else(|| DEFAULT_REGION.to_string()));
    let client_config = aws_config::defaults(BehaviorVersion::latest())
        .region(region.clone())
        .credentials_provider(creds)
        .request_checksum_calculation(RequestChecksumCalculation::WhenRequired)
        .response_checksum_validation(aws_sdk_s3::config::ResponseChecksumValidation::WhenRequired)
        .load()
        .await;
    let s3_config = aws_sdk_s3::config::Builder::from(&client_config)
        .region(region)
        .endpoint_url(endpoint)
        .force_path_style(force_path_style)
        .build();

    Ok(Client::from_conf(s3_config))
}

fn required_key<'a>(config: &'a HashMap<String, String>, key: &str) -> Result<&'a str, BlobError> {
    config.get(key).map(String::as_str).ok_or_else(|| {
        BlobError::OperatorCreationFailed(format!("blob backend config is missing {key}"))
    })
}

pub async fn make_bucket(bucket: &str, config: &HashMap<String, String>) -> Result<(), BlobError> {
    let region = config
        .get("region")
        .cloned()
        .unwrap_or_else(|| DEFAULT_REGION.to_string());
    let s3_client = create_s3_client(
        required_key(config, "endpoint")?,
        Some(region.clone()),
        required_key(config, "access_key_id")?,
        required_key(config, "secret_access_key")?,
        config
            .get("force_path_style")
            .map(|val| val.parse::<bool>().unwrap_or(true))
            .unwrap_or(true),
    )
    .await?;

    if s3_client
        .get_bucket_location()
        .bucket(bucket)
        .send()
        .await
        .is_ok()
    {
        return Ok(());
    }

    let mut request = s3_client.create_bucket().bucket(bucket);
    if let Some(constraint) = location_constraint(&region) {
        request = request.create_bucket_configuration(
            CreateBucketConfiguration::builder()
                .location_constraint(constraint)
                .build(),
        );
    }

    match request.send().await {
        Ok(_) => Ok(()),
        Err(err) => match err.as_service_error() {
            // A racing creator won: the bucket is ours, or at least reachable.
            Some(service) if service.is_bucket_already_owned_by_you() => Ok(()),
            Some(service) if service.is_bucket_already_exists() => s3_client
                .get_bucket_location()
                .bucket(bucket)
                .send()
                .await
                .map(|_| ())
                .map_err(|_| BlobError::MakeBucketError(err.to_string())),
            _ => Err(BlobError::MakeBucketError(err.to_string())),
        },
    }
}

fn location_constraint(region: &str) -> Option<BucketLocationConstraint> {
    (region != IMPLICIT_REGION).then(|| BucketLocationConstraint::from(region))
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_s3::error::SdkError;
    use tokio::net::TcpListener;

    // The AWS SDK ships no HTTPS connector unless `default-https-client` is on, and
    // its absence only shows up at runtime. A TLS handshake against a socket that
    // hangs up must fail while dispatching, not while constructing the request:
    // a ConstructionFailure here means the connector was dropped from the build.
    #[tokio::test]
    async fn https_connector_present() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                drop(stream);
            }
        });

        let client = create_s3_client(&format!("https://{addr}"), None, "key", "secret", true)
            .await
            .unwrap();

        let err = client
            .get_bucket_location()
            .bucket("bucket")
            .send()
            .await
            .expect_err("handshake against a closed socket must fail");

        assert!(matches!(err, SdkError::DispatchFailure(_)), "{err:?}");
    }

    #[test]
    fn omits_default_constraint() {
        // us-east-1 must stay implicit; every other region must be named.
        assert!(location_constraint("us-east-1").is_none());
        assert_eq!(
            location_constraint("eu-central-1"),
            Some(BucketLocationConstraint::EuCentral1)
        );
    }
}
