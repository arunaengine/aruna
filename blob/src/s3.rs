use aruna_core::errors::BlobError;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region, RequestChecksumCalculation};
use std::collections::HashMap;

pub async fn create_s3_client(
    endpoint: &str,
    region: Option<String>,
    access_key_id: &str,
    secret_key: &str,
    force_path_style: bool,
) -> Result<Client, BlobError> {
    let creds = Credentials::new(access_key_id, secret_key, None, None, "Aruna_v3");
    let client_config = aws_config::defaults(BehaviorVersion::latest())
        .credentials_provider(creds)
        .request_checksum_calculation(RequestChecksumCalculation::WhenRequired)
        .response_checksum_validation(aws_sdk_s3::config::ResponseChecksumValidation::WhenRequired)
        .load()
        .await;
    let s3_config = aws_sdk_s3::config::Builder::from(&client_config)
        .region(Region::new(region.unwrap_or("eu-central-1".to_string())))
        .endpoint_url(endpoint)
        .force_path_style(force_path_style)
        .build();

    Ok(Client::from_conf(s3_config))
}

pub async fn make_bucket(bucket: &str, config: &HashMap<String, String>) -> Result<(), BlobError> {
    let s3_client = create_s3_client(
        config
            .get("endpoint")
            .expect("Config is missing endpoint URL"),
        config.get("region").cloned(),
        config
            .get("access_key_id")
            .expect("Config is missing access key id"),
        config
            .get("secret_access_key")
            .expect("Config is missing secret access key"),
        config
            .get("force_path_style")
            .map(|val| val.parse::<bool>().unwrap_or(false))
            .unwrap_or(false),
    )
    .await?;

    match s3_client.get_bucket_location().bucket(bucket).send().await {
        Ok(_) => Ok(()),
        Err(_) => match s3_client.create_bucket().bucket(bucket).send().await {
            Ok(_) => Ok(()),
            Err(err) => Err(BlobError::MakeBucketError(err.to_string())),
        },
    }
}
