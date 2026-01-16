use crate::auth::permission_handler::PermissionHandler;
use crate::auth::token_handler::{Action, Intent};
use crate::caching::cache::Cache;
use crate::database::dsls::endpoint_dsl::{Endpoint, HostConfig};
use crate::database::enums::{DataProxyFeature, ObjectMapping, ReplicationType};
use crate::middlelayer::db_handler::DatabaseHandler;
use crate::middlelayer::endpoints_request_types::GetEP;
use anyhow::{anyhow, Result};
use aruna_rust_api::api::dataproxy::services::v2::dataproxy_user_service_client::DataproxyUserServiceClient;
use aruna_rust_api::api::dataproxy::services::v2::{
    CreateOrUpdateCredentialsRequest, GetCredentialsRequest, GetCredentialsResponse,
};
use aruna_rust_api::api::storage::services::v2::get_endpoint_request::Endpoint as APIEndpointEnum;
use aruna_rust_api::api::storage::services::v2::{
    GetDownloadUrlRequest, GetEndpointRequest, GetUploadUrlRequest,
};
use aws_config::BehaviorVersion;
use aws_sdk_s3::config::Credentials;
use aws_sdk_s3::Client;
use aws_types::region::Region;
use diesel_ulid::DieselUlid;
use itertools::Itertools;
use log::debug;
use reqsign::{AwsCredential, AwsV4Signer};
use reqwest::Method;
use std::str::FromStr;
use std::sync::Arc;
use tonic::metadata::{AsciiMetadataKey, AsciiMetadataValue};
use tonic::transport::{Channel, ClientTlsConfig};
use tonic::Request;
use url::Url;

pub struct PresignedUpload(pub GetUploadUrlRequest);
pub struct PresignedDownload(pub GetDownloadUrlRequest);
impl DatabaseHandler {
    pub async fn get_presigned_download_with_credentials(
        &self,
        authorizer: Arc<PermissionHandler>,
        request: PresignedDownload,
        user_id: DieselUlid,
        token_id: Option<DieselUlid>,
        associated_project: DieselUlid,
        endpoint: Endpoint,
    ) -> Result<(String, GetCredentialsResponse)> {
        let object_id = request.get_id()?;

        let (_project_id, bucket_name, key) = DatabaseHandler::get_path_for_associated_project(
            associated_project,
            object_id,
            self.cache.clone(),
        )
        .await?;

        let (_, endpoint_s3_url, ssl, credentials) = DatabaseHandler::get_or_create_credentials(
            authorizer.clone(),
            user_id,
            token_id,
            endpoint.clone(),
            true,
        )
        .await?;
        let url = sign_download_url(
            &credentials.access_key,
            &credentials.secret_key,
            ssl,
            &bucket_name,
            &key,
            &endpoint_s3_url,
        )?;
        Ok((url, credentials))
    }
    pub async fn get_presigned_download(
        &self,
        cache: Arc<Cache>,
        authorizer: Arc<PermissionHandler>,
        request: PresignedDownload,
        user_id: DieselUlid,
        token: Option<DieselUlid>,
    ) -> Result<String> {
        let object_id = request.get_id()?;
        let (project_id, bucket_name, key) =
            DatabaseHandler::get_path(object_id, cache.clone()).await?;
        let endpoint = self.get_fullsync_endpoint(project_id).await?;

        // Not sure if this is needed
        // Check if user trusts endpoint
        let user = cache
            .get_user(&user_id)
            .ok_or_else(|| anyhow!("User not found"))?;
        if !user
            .attributes
            .0
            .trusted_endpoints
            .contains_key(&endpoint.id)
        {
            return Err(anyhow!("User does not trust endpoint"));
        }

        let (_, endpoint_s3_url, ssl, credentials) =
            DatabaseHandler::get_or_create_credentials(authorizer, user_id, token, endpoint, true)
                .await?;
        let url = sign_download_url(
            &credentials.access_key,
            &credentials.secret_key,
            ssl,
            &bucket_name,
            &key,
            &endpoint_s3_url,
        )?;
        Ok(url)
    }
    pub async fn get_presigend_upload(
        &self,
        cache: Arc<Cache>,
        request: PresignedUpload,
        authorizer: Arc<PermissionHandler>,
        user_id: DieselUlid,
        token: Option<DieselUlid>,
    ) -> Result<(String, Option<String>)> {
        let object_id = request.get_id()?;
        let multipart = request.get_multipart();
        let part_nr = request.get_parts()?;

        let (project_id, bucket_name, key) =
            DatabaseHandler::get_path(object_id, cache.clone()).await?;

        let endpoint = self.get_fullsync_endpoint(project_id).await?;
        let (_, endpoint_s3_url, ssl, credentials) =
            DatabaseHandler::get_or_create_credentials(authorizer, user_id, token, endpoint, true)
                .await?;

        let upload_id = if let Some(upload_id) = request.get_upload_id() {
            Some(upload_id)
        } else if multipart && part_nr == 1 {
            DatabaseHandler::impersonated_multi_upload_init(
                &credentials.access_key,
                &credentials.secret_key,
                &endpoint_s3_url,
                &bucket_name,
                &key,
            )
            .await?
        } else {
            None
        };

        let signed_url = sign_url(
            Method::PUT,
            &credentials.access_key,
            &credentials.secret_key,
            ssl,
            multipart,
            part_nr,
            upload_id.clone(),
            &bucket_name,
            &key,
            &endpoint_s3_url,
            604800,
        )?;

        Ok((signed_url, upload_id))
    }

    pub async fn get_path(
        object_id: DieselUlid,
        cache: Arc<Cache>,
    ) -> Result<(DieselUlid, String, String)> {
        let paths = cache.upstream_dfs_iterative(&object_id)?;
        let mut path: (DieselUlid, String, String) =
            (DieselUlid::default(), String::new(), String::new());
        if let Some(path_components) = paths.into_iter().next() {
            let mut project_id = DieselUlid::default();
            let mut project_name = String::new();
            let mut collection_name = String::new();
            let mut dataset_name = String::new();
            let mut object_name = String::new();
            for component in path_components {
                match component {
                    ObjectMapping::PROJECT(id) => {
                        project_name = cache
                            .get_object(&id)
                            .ok_or_else(|| anyhow!("Parent not found"))?
                            .object
                            .name;
                        project_id = id
                    }
                    ObjectMapping::COLLECTION(id) => {
                        collection_name = cache
                            .get_object(&id)
                            .ok_or_else(|| anyhow!("Parent not found"))?
                            .object
                            .name
                    }
                    ObjectMapping::DATASET(id) => {
                        dataset_name = cache
                            .get_object(&id)
                            .ok_or_else(|| anyhow!("Parent not found"))?
                            .object
                            .name
                    }
                    ObjectMapping::OBJECT(id) => {
                        object_name = cache
                            .get_object(&id)
                            .ok_or_else(|| anyhow!("Parent not found"))?
                            .object
                            .name
                    }
                }
            }
            let key = if project_name.is_empty() || object_name.is_empty() {
                return Err(anyhow!("No project or object found"));
            } else {
                match (!collection_name.is_empty(), !dataset_name.is_empty()) {
                    (true, true) => {
                        format!("{collection_name}/{dataset_name}/{object_name}")
                    }
                    (false, true) => format!("{dataset_name}/{object_name}"),
                    (true, false) => {
                        format!("{collection_name}/{object_name}")
                    }
                    (false, false) => object_name,
                }
            };
            path = (project_id, project_name, key);
        }
        Ok(path)
    }
    async fn get_path_for_associated_project(
        project_id: DieselUlid,
        object_id: DieselUlid,
        cache: Arc<Cache>,
    ) -> Result<(DieselUlid, String, String)> {
        let paths = cache.upstream_dfs_iterative(&object_id)?;
        let path_components = paths
            .iter()
            .find(|c| c.iter().contains(&ObjectMapping::PROJECT(project_id)))
            .ok_or_else(|| {
                anyhow!("No path found for project {project_id} and object {object_id}")
            })?;
        let mut project_id = DieselUlid::default();
        let mut project_name = String::new();
        let mut collection_name = String::new();
        let mut dataset_name = String::new();
        let mut object_name = String::new();
        for component in path_components {
            match component {
                ObjectMapping::PROJECT(id) => {
                    project_name = cache
                        .get_object(id)
                        .ok_or_else(|| anyhow!("Parent not found"))?
                        .object
                        .name;
                    project_id = *id
                }
                ObjectMapping::COLLECTION(id) => {
                    collection_name = cache
                        .get_object(id)
                        .ok_or_else(|| anyhow!("Parent not found"))?
                        .object
                        .name
                }
                ObjectMapping::DATASET(id) => {
                    dataset_name = cache
                        .get_object(id)
                        .ok_or_else(|| anyhow!("Parent not found"))?
                        .object
                        .name
                }
                ObjectMapping::OBJECT(id) => {
                    object_name = cache
                        .get_object(id)
                        .ok_or_else(|| anyhow!("Parent not found"))?
                        .object
                        .name
                }
            }
        }
        let key = if project_name.is_empty() || object_name.is_empty() {
            return Err(anyhow!("No project or object found"));
        } else {
            match (!collection_name.is_empty(), !dataset_name.is_empty()) {
                (true, true) => {
                    format!("{collection_name}/{dataset_name}/{object_name}")
                }
                (false, true) => format!("{dataset_name}/{object_name}"),
                (true, false) => {
                    format!("{collection_name}/{object_name}")
                }
                (false, false) => object_name,
            }
        };
        Ok((project_id, project_name, key))
    }
    pub async fn get_fullsync_endpoint(&self, object_id: DieselUlid) -> Result<Endpoint> {
        // Only gets first endpoint
        let endpoint = *Vec::from_iter(
            self.cache
                .get_object(&object_id)
                .ok_or_else(|| anyhow!("Object not found"))?
                .object
                .endpoints
                .0,
        )
        .iter()
        .find(|(_, ep)| matches!(ep.replication, ReplicationType::FullSync))
        .ok_or_else(|| anyhow!("No full sync endpoint found"))?
        .0;
        // Fetch endpoint from cache/database
        self.get_endpoint(GetEP(GetEndpointRequest {
            endpoint: Some(APIEndpointEnum::EndpointId(endpoint.to_string())),
        }))
        .await
    }

    pub async fn get_or_create_credentials(
        authorizer: Arc<PermissionHandler>,
        user_id: DieselUlid,
        token_id: Option<DieselUlid>,
        project_endpoint: Endpoint,
        allow_create: bool,
    ) -> Result<(String, String, bool, GetCredentialsResponse)> {
        // Get s3 creds with slt:
        // 1. Create short-lived token with intent
        let token_id = token_id.map(|t| t.to_string());
        let slt = authorizer.token_handler.sign_dataproxy_slt(
            &user_id,
            token_id,
            Some(Intent {
                target: project_endpoint.id,
                action: Action::CreateSecrets,
            }),
        )?;

        // 2. Request S3 credentials from Dataproxy
        let mut ssl: bool = true;
        let mut endpoint_host_url: String = String::new();
        let mut endpoint_s3_url: String = String::new();
        for endpoint_config in project_endpoint.host_config.0 .0 {
            match endpoint_config {
                HostConfig {
                    feature: DataProxyFeature::S3,
                    is_primary: true,
                    ..
                } => {
                    endpoint_s3_url = endpoint_config.url;
                    ssl = endpoint_config.ssl;
                }
                HostConfig {
                    feature: DataProxyFeature::GRPC,
                    is_primary: true,
                    ..
                } => {
                    endpoint_host_url = endpoint_config.url;
                }
                _ => continue,
            };
            if !endpoint_s3_url.is_empty() && !endpoint_host_url.is_empty() {
                break;
            }
        }
        if endpoint_host_url.is_empty() {
            return Err(anyhow!("No valid endpoint config found"));
        }

        // Check if dataproxy host url is tls
        let dp_endpoint = if endpoint_host_url.starts_with("https") {
            Channel::from_shared(endpoint_host_url.clone())
                .map_err(|_| tonic::Status::internal("Could not connect to Dataproxy"))?
                .tls_config(ClientTlsConfig::new())
                .map_err(|_| tonic::Status::internal("Could not connect to Dataproxy"))?
        } else {
            Channel::from_shared(endpoint_host_url.clone())
                .map_err(|_| tonic::Status::internal("Could not connect to Dataproxy"))?
        };
        let mut dp_conn = DataproxyUserServiceClient::connect(dp_endpoint).await?;
        debug!("Opened connection to DataProxy");

        // 3. Create GetCredentialsRequest with one-shot token in header ...
        let mut credentials_request = Request::new(GetCredentialsRequest {});
        credentials_request.metadata_mut().append(
            AsciiMetadataKey::from_bytes("Authorization".as_bytes())?,
            AsciiMetadataValue::try_from(format!("Bearer {slt}"))?,
        );

        debug!("Send Request to DataProxy");
        let response = match dp_conn.get_credentials(credentials_request).await {
            Ok(response) => response.into_inner(),
            Err(e) => {
                if e.code() == tonic::Code::Unauthenticated && allow_create {
                    debug!("Credentials not available, creating a new one.");
                    let mut credentials_request = Request::new(CreateOrUpdateCredentialsRequest {});
                    credentials_request.metadata_mut().append(
                        AsciiMetadataKey::from_bytes("Authorization".as_bytes())?,
                        AsciiMetadataValue::try_from(format!("Bearer {slt}"))?,
                    );
                    let response = dp_conn
                        .create_or_update_credentials(credentials_request)
                        .await?
                        .into_inner();
                    GetCredentialsResponse {
                        access_key: response.access_key,
                        secret_key: response.secret_key,
                    }
                } else {
                    log::error!("Error getting credentials from Dataproxy: {e}");
                    return Err(anyhow!("Error getting credentials from Dataproxy: {}", e));
                }
            }
        };
        debug!("{response:#?}");

        Ok((endpoint_host_url, endpoint_s3_url, ssl, response))
    }

    async fn impersonated_multi_upload_init(
        access_key: &str,
        secret_key: &str,
        endpoint_host_url: &str,
        bucket_name: &str,
        key: &str,
    ) -> Result<Option<String>> {
        // Impersonate User and InitMultiPartUpload via S3 and endpoint_host_url for multipart uploads
        let creds = Credentials::new(
            access_key,
            secret_key,
            None,
            None,
            "ARUNA_SERVER", // Endpoint name?
        );
        let config = aws_config::defaults(BehaviorVersion::v2025_08_07())
            .credentials_provider(creds)
            .load()
            .await;
        let s3_config = aws_sdk_s3::config::Builder::from(&config)
            .region(Region::new("RegionOne"))
            .endpoint_url(endpoint_host_url)
            .build();

        let s3_client = Client::from_conf(s3_config);

        let upload_id = s3_client
            .create_multipart_upload()
            .set_bucket(Some(bucket_name.to_string()))
            .set_key(Some(key.to_string()))
            .send()
            .await?
            .upload_id()
            .map(|id| id.to_string());
        Ok(upload_id)
    }
}
impl PresignedDownload {
    pub fn get_id(&self) -> Result<DieselUlid> {
        Ok(DieselUlid::from_str(&self.0.object_id)?)
    }
}

impl PresignedUpload {
    pub fn get_id(&self) -> Result<DieselUlid> {
        Ok(DieselUlid::from_str(&self.0.object_id)?)
    }
    pub fn get_multipart(&self) -> bool {
        self.0.multipart
    }
    pub fn get_parts(&self) -> Result<i32> {
        let multipart = self.get_multipart();
        let part_number = self.0.part_number;
        let parts = match (part_number, multipart) {
            (n, true) if !(1..=10000).contains(&n) => {
                return Err(anyhow!("Invalid part number provided for multipart upload",))
            }
            (n, true) => n,
            (_, false) => 1,
        };
        Ok(parts)
    }
    pub fn get_upload_id(&self) -> Option<String> {
        if self.0.upload_id.is_empty() {
            None
        } else {
            Some(self.0.upload_id.clone())
        }
    }
}

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
fn sign_url(
    method: Method,
    access_key: &str,
    secret_key: &str,
    ssl: bool,
    multipart: bool,
    part_number: i32,
    upload_id: Option<String>,
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
        let upload_id = upload_id
            .ok_or_else(|| anyhow!("No upload id provided for multipart presigned url"))?;
        Url::parse(&format!(
            "{protocol}{bucket}.{endpoint_sanitized}/{key}?partNumber={part_number}&uploadId={upload_id}"
        ))?
    } else {
        Url::parse(&format!("{protocol}{bucket}.{endpoint_sanitized}/{key}"))?
    };

    let mut req = reqwest::Request::new(method, url);

    // Signing request with Signer
    signer.sign_query(
        &mut req,
        std::time::Duration::new(duration as u64, 0), // Sec, nano
        &AwsCredential {
            access_key_id: access_key.to_string(),
            secret_access_key: secret_key.to_string(),
            session_token: None,
            expires_in: None,
        },
    )?;
    Ok(req.url().to_string())
}

/// Convenience wrapper function for sign_url(...) to reduce unused parameters for download url.
fn sign_download_url(
    access_key: &str,
    secret_key: &str,
    ssl: bool,
    bucket: &str,
    key: &str,
    endpoint: &str,
) -> Result<String> {
    sign_url(
        Method::GET,
        access_key,
        secret_key,
        ssl,
        false,
        0,
        None,
        bucket,
        key,
        endpoint,
        604800, //Note: Default 1 week until requests allow custom duration
    )
}
