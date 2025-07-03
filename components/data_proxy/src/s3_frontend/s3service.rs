use super::data_handler::DataHandler;
use super::utils::buffered_s3_sink::BufferedS3Sink;
use super::utils::ranges::calculate_ranges;
use crate::bundler::bundle_helper::get_bundle;
use crate::caching::cache::Cache;
use crate::data_backends::storage_backend::StorageBackend;
use crate::s3_frontend::utils::list_objects::list_response;
use crate::structs::CheckAccessResult;
use crate::structs::NewOrExistingObject;
use crate::structs::Object as ProxyObject;
use crate::structs::ObjectsState;
use crate::structs::PartETag;
use crate::structs::TypedRelation;
use crate::CONFIG;
use anyhow::Result;
use aruna_rust_api::api::storage::models::v2::Hash;
use aruna_rust_api::api::storage::models::v2::Hashalgorithm;
use aruna_rust_api::api::storage::models::v2::Status;
use base64::engine::general_purpose;
use base64::Engine;
use bytes::BufMut;
use bytes::BytesMut;
use futures_util::TryStreamExt;
use http::HeaderName;
use http::HeaderValue;
use md5::{Digest, Md5};
use pithos_lib::helpers::footer_parser::Footer;
use pithos_lib::helpers::footer_parser::FooterParser;
use pithos_lib::helpers::footer_parser::FooterParserState;
use pithos_lib::helpers::notifications::Message as PithosMessage;
use pithos_lib::streamreadwrite::GenericStreamReadWriter;
use pithos_lib::transformer::ReadWriter;
use pithos_lib::transformers::async_sender_sink::AsyncSenderSink;
use pithos_lib::transformers::decrypt_with_parts::ChaCha20DecParts;
use pithos_lib::transformers::encrypt::ChaCha20Enc;
use pithos_lib::transformers::filter::Filter;
use pithos_lib::transformers::footer::FooterGenerator;
use pithos_lib::transformers::hashing_transformer::HashingTransformer;
use pithos_lib::transformers::pithos_comp_enc::PithosTransformer;
use pithos_lib::transformers::size_probe::SizeProbe;
use pithos_lib::transformers::zstd_comp::ZstdEnc;
use pithos_lib::transformers::zstd_decomp::ZstdDec;
use s3s::dto::*;
use s3s::s3_error;
use s3s::S3Error;
use s3s::S3Request;
use s3s::S3Response;
use s3s::S3Result;
use s3s::S3;
use sha2::Sha256;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::Debug;
use std::sync::Arc;
use tokio::pin;
use tracing::debug;
use tracing::error;
use tracing::info_span;
use tracing::trace;
use tracing::warn;
use tracing::Instrument;

pub struct ArunaS3Service {
    backend: Arc<Box<dyn StorageBackend>>,
    cache: Arc<Cache>,
}

impl Debug for ArunaS3Service {
    #[tracing::instrument(level = "trace", skip(self, f))]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArunaS3Service").finish()
    }
}

impl ArunaS3Service {
    #[tracing::instrument(level = "trace", skip(backend, cache))]
    pub async fn new(backend: Arc<Box<dyn StorageBackend>>, cache: Arc<Cache>) -> Result<Self> {
        Ok(ArunaS3Service {
            backend: backend.clone(),
            cache,
        })
    }

    #[tracing::instrument(level = "trace", skip(object, headers))]
    async fn put_empty_object(
        &self,
        object: ProxyObject,
        headers: Option<HashMap<String, String>>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        let md5 = Md5::new().finalize();
        let sha256 = Sha256::new().finalize();

        let output = PutObjectOutput {
            e_tag: Some(format!("{md5:x}")),
            checksum_sha256: Some(format!("{sha256:x}")),
            version_id: Some(object.id.to_string()),
            ..Default::default()
        };

        self.cache.insert_temp_resource(object).await.map_err(|_| {
            error!(error = "Unable to temporarily cache empty object");
            s3_error!(InternalError, "Unable to temporarily cache empty object")
        })?;

        let mut resp = S3Response::new(output);
        if let Some(headers) = headers {
            for (k, v) in headers {
                resp.headers.insert(
                    HeaderName::from_bytes(k.as_bytes())
                        .map_err(|_| s3_error!(InternalError, "Unable to parse header name"))?,
                    HeaderValue::from_str(&v)
                        .map_err(|_| s3_error!(InternalError, "Unable to parse header value"))?,
                );
            }
        }

        Ok(resp)
    }
}

#[async_trait::async_trait]
impl S3 for ArunaS3Service {
    #[tracing::instrument(err)]
    #[allow(clippy::blocks_in_conditions)]
    async fn complete_multipart_upload(
        &self,
        req: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        let CheckAccessResult {
            user_state,
            objects_state,
            ..
        } = req
            .extensions
            .get::<CheckAccessResult>()
            .cloned()
            .ok_or_else(|| {
                error!(error = "Missing data context");
                s3_error!(UnexpectedContent, "Missing data context")
            })?;

        let impersonating_token =
            user_state.sign_impersonating_token(self.cache.auth.read().await.as_ref());

        let (object, old_location) = objects_state.extract_object()?;
        let mut old_location = old_location.ok_or_else(|| {
            error!(error = "Unable to extract object location");
            s3_error!(InternalError, "Unable to extract object location")
        })?;

        let parts = match req.input.multipart_upload {
            Some(parts) => parts.parts.ok_or_else(|| {
                error!(error = "Parts must be specified");
                s3_error!(InvalidPart, "Parts must be specified")
            }),
            None => {
                error!("parts is none");
                return Err(s3_error!(InvalidPart, "Parts must be specified"));
            }
        }?;

        let etag_parts = parts
            .into_iter()
            .map(|a| {
                Ok(PartETag {
                    part_number: a.part_number.ok_or_else(|| {
                        error!(error = "part_number must be specified");
                        s3_error!(InvalidPart, "part_number must be specified")
                    })?,
                    etag: a.e_tag.ok_or_else(|| {
                        error!(error = "etag must be specified");
                        s3_error!(InvalidPart, "etag must be specified")
                    })?,
                })
            })
            .collect::<Result<Vec<PartETag>, S3Error>>()?;

        let upload_id = old_location
            .upload_id
            .as_ref()
            .ok_or_else(|| {
                error!(error = "Upload id must be specified");
                s3_error!(InvalidPart, "Upload id must be specified")
            })?
            .to_string();

        let parts = self.cache.get_parts(&upload_id);

        let mut cumulative_size = 0;
        let mut disk_size = 0;
        'outer: for part in parts {
            for etag in etag_parts.iter() {
                if part.part_number == etag.part_number as u64 {
                    cumulative_size += part.raw_size;
                    disk_size += part.size;
                    continue 'outer;
                }
            }
            self.cache
                .delete_part(upload_id.to_string(), part.part_number)
                .await
                .map_err(|_| {
                    error!(error = "Unable to delete part");
                    s3_error!(InternalError, "Unable to delete part")
                })?;
        }

        // Does this object exists (including object id etc)
        //req.input.multipart_upload.unwrap().
        self.backend
            .clone()
            .finish_multipart_upload(old_location.clone(), etag_parts, upload_id.to_string())
            .await
            .map_err(|_| {
                error!(error = "Unable to finish upload");
                s3_error!(InternalError, "Unable to finish upload")
            })?;

        let response = CompleteMultipartUploadOutput {
            e_tag: Some(format!("-{}", object.id)),
            ..Default::default()
        };

        old_location.disk_content_len = disk_size as i64;
        old_location.raw_content_len = cumulative_size as i64;

        self.cache
            .update_location(object.id, old_location.clone())
            .await
            .map_err(|_| {
                error!(error = "Unable to update location");
                s3_error!(InternalError, "Unable to update location")
            })?;

        if let Some(handler) = self.cache.aruna_client.read().await.as_ref() {
            if let Some(token) = &impersonating_token {
                // Set id of new location to object id to satisfy FK constraint
                let _ = handler
                    .finish_object(object.id, cumulative_size as i64, vec![], token)
                    .await
                    .map_err(|_| {
                        error!(error = "Unable to finish object");
                        s3_error!(InternalError, "Unable to create object")
                    })?;
            }
        }

        tokio::spawn(DataHandler::finalize_location(
            object,
            self.cache.clone(),
            self.backend.clone(),
            old_location,
            Some(objects_state.try_slice()?),
        ));
        debug!(?response);
        Ok(S3Response::new(response))
    }

    #[tracing::instrument(err)]
    #[allow(clippy::blocks_in_conditions)]
    async fn create_bucket(
        &self,
        req: S3Request<CreateBucketInput>,
    ) -> S3Result<S3Response<CreateBucketOutput>> {
        let data = req.extensions.get::<CheckAccessResult>().cloned();

        let mut new_object = ProxyObject::from(req.input);

        if let Some(client) = self.cache.aruna_client.read().await.as_ref() {
            let CheckAccessResult { user_state, .. } = data.ok_or_else(|| {
                error!(error = "Missing data context");
                s3_error!(InternalError, "Internal Error")
            })?;

            let impersonating_token = user_state
                .sign_impersonating_token(self.cache.auth.read().await.as_ref())
                .ok_or_else(|| {
                    error!(error = "Unauthorized: Impersonating error");
                    s3_error!(NotSignedUp, "Unauthorized: Impersonating error")
                })?;

            // TODO: EndpointInfo
            // -> CreateProject adds matching EndpointId to create project request, and then returns a
            //    correctly parsed Object where Endpoint infos are added
            new_object = client
                .create_project(new_object, &impersonating_token)
                .await
                .map_err(|_| {
                    error!(error = "Unable to create project");
                    s3_error!(InternalError, "[BACKEND] Unable to create project")
                })?;
        }
        let output = CreateBucketOutput {
            location: Some(new_object.name.to_string()),
        };

        self.cache.upsert_object(new_object).await.map_err(|_| {
            error!(error = "Unable to cache new bucket");
            s3_error!(InternalError, "Unable to cache new bucket")
        })?;

        debug!(?output);
        Ok(S3Response::new(output))
    }

    #[tracing::instrument(err)]
    #[allow(clippy::blocks_in_conditions)]
    async fn create_multipart_upload(
        &self,
        req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        let CheckAccessResult {
            objects_state,
            user_state,
            ..
        } = req
            .extensions
            .get::<CheckAccessResult>()
            .cloned()
            .ok_or_else(|| {
                error!(error = "Missing data context");
                s3_error!(UnexpectedContent, "Missing data context")
            })?;

        let impersonating_token =
            user_state.sign_impersonating_token(self.cache.auth.read().await.as_ref());

        let (states, _) = objects_state.require_regular()?;

        let (_, collection, dataset, object, location_state) = states.to_new_or_existing()?;

        trace!(?collection, ?dataset, ?object);

        let mut new_object = match &object {
            NewOrExistingObject::Existing(ob) => {
                if ob.object_status == Status::Initializing {
                    trace!("Object is initializing");
                    ob.clone()
                } else {
                    let mut new_revision = if let Some(handler) =
                        self.cache.aruna_client.read().await.as_ref()
                    {
                        if let Some(token) = &impersonating_token {
                            handler
                                .init_object_update(ob.clone(), token, true)
                                .await
                                .map_err(|_| {
                                    error!(error = "Object update failed");
                                    s3_error!(InternalError, "Object update failed")
                                })?
                        } else {
                            error!("missing impersonating token");
                            return Err(s3_error!(InternalError, "Token creation failed"));
                        }
                    } else {
                        //TODO: Enable offline mode
                        error!("ArunaServer client not available");
                        return Err(s3_error!(InternalError, "ArunaServer client not available"));
                    };
                    new_revision.hashes = HashMap::default();
                    new_revision.synced = false;
                    new_revision.children = None;
                    new_revision.dynamic = false;
                    new_revision
                }
            }
            NewOrExistingObject::Missing(object) => object.clone(),
            NewOrExistingObject::None => {
                return Err(s3_error!(
                    InvalidObjectState,
                    "Object in invalid state: None"
                ))
            }
        };

        trace!(?new_object);

        let mut location = self
            .backend
            .initialize_location(&new_object, None, location_state, true)
            .await
            .map_err(|_| {
                error!(error = "Unable to create object_location");
                s3_error!(InternalError, "Unable to create object_location")
            })?;
        trace!(?location);

        let init_response = self
            .backend
            .clone()
            .init_multipart_upload(location.clone())
            .await
            .map_err(|_| {
                error!(error = "Unable to initialize multi-part");
                s3_error!(InvalidArgument, "Unable to initialize multi-part")
            })?;

        location.upload_id = Some(init_response.to_string());

        let mut collection_id = None;
        if let NewOrExistingObject::Missing(collection) = collection {
            if let Some(handler) = self.cache.aruna_client.read().await.as_ref() {
                if let Some(token) = &impersonating_token {
                    let col = handler
                        .create_collection(collection, token)
                        .await
                        .map_err(|_| {
                            error!(error = "Unable to create collection");
                            s3_error!(InternalError, "Unable to create collection")
                        })?;
                    collection_id = Some(col.id)
                }
            }
        }

        let mut dataset_id = None;
        if let NewOrExistingObject::Missing(mut dataset) = dataset {
            if let Some(handler) = self.cache.aruna_client.read().await.as_ref() {
                if let Some(token) = &impersonating_token {
                    if let Some(collection_id) = collection_id {
                        dataset.parents = Some(HashSet::from_iter([TypedRelation::Collection(
                            collection_id,
                        )]));
                    }
                    let dataset = handler.create_dataset(dataset, token).await.map_err(|_| {
                        error!(error = "Unable to create dataset");
                        s3_error!(InternalError, "Unable to create dataset")
                    })?;
                    dataset_id = Some(dataset.id);
                }
            }
        }

        if let Some(dataset_id) = dataset_id {
            new_object.parents = Some(HashSet::from_iter([TypedRelation::Dataset(dataset_id)]));
        } else if let Some(collection_id) = collection_id {
            new_object.parents = Some(HashSet::from_iter([TypedRelation::Collection(
                collection_id,
            )]));
        }

        let mut object_id = new_object.id;
        if let NewOrExistingObject::Missing(_) = &object {
            if let Some(handler) = self.cache.aruna_client.read().await.as_ref() {
                if let Some(token) = &impersonating_token {
                    let server_object = handler
                        .create_object(new_object.clone(), token)
                        .await
                        .map_err(|_| {
                            error!(error = "Unable to create object");
                            s3_error!(InternalError, "Unable to create object")
                        })?;
                    object_id = server_object.id;
                }
            }
        } else {
            self.cache.upsert_object(new_object).await.map_err(|_| {
                error!(error = "Unable to cache new object");
                s3_error!(InternalError, "Unable to cache new object")
            })?;
        }

        self.cache
            .add_location_with_binding(object_id, location)
            .await
            .map_err(|e| {
                error!(error = ?e, msg = "Unable to add location binding to object");
                s3_error!(InternalError, "Unable to cache new object")
            })?;

        let output = CreateMultipartUploadOutput {
            key: Some(req.input.key),
            bucket: Some(req.input.bucket),
            upload_id: Some(init_response),
            ..Default::default()
        };
        debug!(?output);
        Ok(S3Response::new(output))
    }

    #[tracing::instrument(err)]
    #[allow(clippy::blocks_in_conditions)]
    async fn list_parts(
        &self,
        req: S3Request<ListPartsInput>,
    ) -> S3Result<S3Response<ListPartsOutput>> {
        let CheckAccessResult { headers, .. } = req
            .extensions
            .get::<CheckAccessResult>()
            .cloned()
            .ok_or_else(|| {
                error!(error = "No context found");
                s3_error!(InternalError, "No context found")
            })?;

        let max_parts = match req.input.max_parts {
            Some(k) if k < 1000 => k as usize,
            _ => 1000usize,
        };
        let start_at: u64 = match &req.input.part_number_marker {
            Some(marker) => {
                let next_part_number = marker.parse().map_err(|_| {
                    error!(error = "Invalid part number marker", marker);
                    s3_error!(InvalidArgument, "Invalid part number marker")
                })?;

                if next_part_number < 10000 {
                    next_part_number
                } else {
                    error!(error = "Invalid part number marker", marker);
                    return Err(s3_error!(InvalidArgument, "Invalid part number marker"));
                }
            }
            None => 1,
        };
        let (parts, next_marker, is_truncated) =
            self.cache
                .list_parts(&req.input.upload_id, start_at, max_parts);

        let output = ListPartsOutput {
            bucket: Some(req.input.bucket),
            is_truncated: Some(is_truncated),
            key: Some(req.input.key),
            max_parts: req.input.max_parts,
            next_part_number_marker: next_marker,
            owner: None, //Todo?
            part_number_marker: req.input.part_number_marker,
            parts: Some(parts),
            upload_id: Some(req.input.upload_id),
            ..Default::default()
        };
        debug!(?output);
        let mut resp = S3Response::new(output);
        if let Some(headers) = headers {
            for (k, v) in headers {
                resp.headers.insert(
                    HeaderName::from_bytes(k.as_bytes())
                        .map_err(|_| s3_error!(InternalError, "Unable to parse header name"))?,
                    HeaderValue::from_str(&v)
                        .map_err(|_| s3_error!(InternalError, "Unable to parse header value"))?,
                );
            }
        }
        Ok(resp)
    }

    #[tracing::instrument(err)]
    #[allow(clippy::blocks_in_conditions)]
    async fn get_object(
        &self,
        req: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        let CheckAccessResult {
            objects_state,
            headers,
            ..
        } = req
            .extensions
            .get::<CheckAccessResult>()
            .cloned()
            .ok_or_else(|| {
                error!(error = "Missing data context");
                s3_error!(InternalError, "No context found")
            })?;

        let ObjectsState::Regular { states, location } = objects_state else {
            let (levels, name) = match objects_state {
                ObjectsState::Bundle { bundle, .. } => (
                    self.cache
                        .get_path_levels(bundle.ids.as_slice())
                        .await
                        .map_err(|_| {
                            error!(error = "Unable to get path levels");
                            s3_error!(InternalError, "Unable to get path levels")
                        })?,
                    format!("{}", bundle.id),
                ),
                ObjectsState::Objects { root, .. } => (
                    self.cache.get_path_levels(&[root.id]).await.map_err(|_| {
                        error!(error = "Unable to get path levels");
                        s3_error!(InternalError, "Unable to get path levels")
                    })?,
                    format!("{}", root.id),
                ),
                _ => return Err(s3_error!(InternalError, "Invalid object state")),
            };

            let body = get_bundle(levels, self.backend.clone()).await;

            let mut resp = S3Response::new(GetObjectOutput {
                body,
                last_modified: None,
                content_disposition: Some(format!(r#"attachment;filename="{name}.tar.gz"#)),
                e_tag: Some(format!("-{name}")),
                ..Default::default()
            });

            resp.headers.insert(
                hyper::header::TRANSFER_ENCODING,
                HeaderValue::from_static("chunked"),
            );

            resp.headers.insert(
                hyper::header::CONTENT_TYPE,
                HeaderValue::from_static("application/octet-stream"),
            );

            return Ok(resp);
        };

        let location = location.ok_or_else(|| {
            error!(error = "Unable to get resource");
            s3_error!(NoSuchKey, "Object not found")
        })?;
        let mut content_length = location.raw_content_len;

        let (sender, receiver) = async_channel::bounded(10);
        let object = states.require_object()?;

        // Gets 128 kb chunks (last 2)

        let footer: Option<Footer> = if location.is_pithos() {
            let (footer_sender, footer_receiver) = async_channel::bounded(1000);
            pin!(footer_receiver);
            self.backend
                .get_object(
                    location.clone(),
                    Some(format!("bytes=-{}", (65536 + 28) * 2)),
                    footer_sender,
                )
                .await
                .map_err(|_| {
                    error!(error = "Unable to get encryption_footer");
                    s3_error!(InternalError, "Unable to get encryption_footer")
                })?;
            let mut output = BytesMut::with_capacity((65536 + 28) * 2);
            while let Ok(Ok(bytes)) = footer_receiver.recv().await {
                output.put(bytes);
            }

            let mut parser = FooterParser::new(&output).unwrap();

            let key = CONFIG.proxy.clone().get_private_key_x25519().map_err(|e| {
                error!(?e, error = "Unable to get private key");
                s3_error!(InternalError, "Unable to get private key")
            })?;
            parser = parser.add_recipient(&key);
            parser = parser.parse().map_err(|e| {
                error!(error = ?e, msg = "Unable to parse footer");
                s3_error!(InternalError, "Unable to parse footer")
            })?;

            if let FooterParserState::Missing(missing_bytes) = parser.state {
                debug!(?missing_bytes, "Missing bytes in footer");
                // Get the footer again with the missing bytes + one full chunk just to be sure
                let buffer_size = ((65536 + 28) * 3) + missing_bytes;

                let (footer_sender, footer_receiver) = async_channel::bounded(1000);
                pin!(footer_receiver);
                self.backend
                    .get_object(
                        location.clone(),
                        Some(format!("bytes=-{buffer_size}")),
                        footer_sender,
                    )
                    .await
                    .map_err(|_| {
                        error!(error = "Unable to get encryption_footer");
                        s3_error!(InternalError, "Unable to get encryption_footer")
                    })?;
                output = BytesMut::with_capacity(buffer_size);
                while let Ok(Ok(bytes)) = footer_receiver.recv().await {
                    output.put(bytes);
                }
                parser = FooterParser::new(&output).unwrap();
                parser = parser.add_recipient(&key);
                parser = parser.parse().map_err(|e| {
                    error!(error = ?e, msg = "Unable to parse footer");
                    s3_error!(InternalError, "Unable to parse footer")
                })?;
            }
            Some(parser.try_into().map_err(|e| {
                error!(?e, error = "Unable to convert footer");
                s3_error!(InternalError, "Unable to convert footer")
            })?)
        } else {
            None
        };

        let parts = if location.is_temporary {
            let mut part_sizes = Vec::new();
            let parts = self
                .cache
                .get_parts(location.upload_id.as_ref().ok_or_else(|| {
                    error!(error = "Upload id must be specified");
                    s3_error!(InvalidPart, "Upload id must be specified")
                })?);

            for parts in parts {
                let full_chunks = (parts.size / (65536 + 28)) * (65536 + 28);
                part_sizes.push(full_chunks);
                if parts.size % (65536 + 28) != 0 {
                    part_sizes.push(parts.size - full_chunks);
                }
            }
            part_sizes
        } else {
            vec![footer
                .as_ref()
                .map(|f| {
                    f.eof_metadata.disk_file_size
                        - f.eof_metadata.toc_len
                        - f.eof_metadata.encryption_len
                        - 73
                })
                .unwrap_or_else(|| location.disk_content_len as u64)]
        };

        trace!("calculating ranges");
        let (query_ranges, edit_list, actual_size, actual_range) = match calculate_ranges(
            req.input.range,
            content_length as u64,
            parts
                .first()
                .copied()
                .unwrap_or(location.disk_content_len as u64),
            footer,
            &location,
        ) {
            Ok((query_ranges, edit_list, actual_size, actual_range)) => {
                (query_ranges, edit_list, actual_size, actual_range)
            }
            Err(err) => {
                error!(error = ?err, "Unable to calculate ranges");
                return Err(s3_error!(InternalError, "Unable to calculate ranges"));
            }
        };

        let (accept_ranges, content_range) = if let Some(query_range) = actual_range {
            content_length = (query_range.to - query_range.from) as i64;
            (
                Some("bytes".to_string()),
                Some(format!(
                    "bytes {}-{}/{}",
                    query_range.from, query_range.to, location.raw_content_len
                )),
            )
        } else {
            (None, None)
        };

        trace!(?edit_list);

        // Spawn get_object to fetch bytes from storage storage
        let backend = self.backend.clone();
        let loc_clone = location.clone();
        trace!(?loc_clone, ?query_ranges, "spawning get_object");
        tokio::spawn(
            async move { backend.get_object(loc_clone, query_ranges, sender).await }
                .instrument(info_span!("get_object")),
        );
        let (final_send, final_rcv) = async_channel::bounded(100);

        let decryption_key = location.get_encryption_key();

        trace!(parts = ?parts);
        // Spawn final part
        tokio::spawn(
            async move {
                pin!(receiver);
                let mut asrw = GenericStreamReadWriter::new_with_sink(
                    receiver,
                    AsyncSenderSink::new(final_send),
                );

                if let Some(key) = decryption_key {
                    asrw = asrw.add_transformer(ChaCha20DecParts::new_with_lengths(
                        key,
                        vec![actual_size],
                    ));
                }

                if location.is_compressed() {
                    asrw = asrw.add_transformer(ZstdDec::new());
                }

                if let Some(edit_list) = edit_list {
                    asrw = asrw.add_transformer(Filter::new_with_edit_list(Some(edit_list)));
                };

                asrw.process().await.map_err(|e| {
                    error!(error = ?e, msg = "Unable to process final part");
                    s3_error!(InternalError, "Internal notifier error")
                })?;

                Ok::<_, anyhow::Error>(())
            }
            .instrument(info_span!("query_data")),
        );

        let body = Some(StreamingBlob::wrap(final_rcv.map_err(|_| {
            error!(error = "Unable to wrap final_rcv");
            s3_error!(InternalError, "Internal processing error")
        })));

        let mime = mime_guess::from_path(object.name.as_str()).first();

        let output = GetObjectOutput {
            body,
            accept_ranges,
            content_range,
            content_length: Some(content_length),
            last_modified: None,
            e_tag: Some(format!("-{}", object.id)),
            version_id: None,
            content_type: mime,
            content_disposition: Some(format!(r#"attachment;filename="{}""#, object.name)),
            ..Default::default()
        };
        debug!(?output);

        let mut resp = S3Response::new(output);
        if let Some(headers) = headers {
            for (k, v) in headers {
                resp.headers.insert(
                    HeaderName::from_bytes(k.as_bytes())
                        .map_err(|_| s3_error!(InternalError, "Unable to parse header name"))?,
                    HeaderValue::from_str(&v)
                        .map_err(|_| s3_error!(InternalError, "Unable to parse header value"))?,
                );
            }
        }
        Ok(resp)
    }

    #[tracing::instrument(err)]
    #[allow(clippy::blocks_in_conditions)]
    async fn head_object(
        &self,
        req: S3Request<HeadObjectInput>,
    ) -> S3Result<S3Response<HeadObjectOutput>> {
        let CheckAccessResult {
            objects_state,
            headers,
            ..
        } = req
            .extensions
            .get::<CheckAccessResult>()
            .cloned()
            .ok_or_else(|| {
                error!(error = "No context found");
                s3_error!(InternalError, "No context found")
            })?;

        if let ObjectsState::Bundle { bundle, .. } = objects_state {
            return Ok(S3Response::new(HeadObjectOutput {
                content_length: None,
                last_modified: Some(
                    time::OffsetDateTime::from_unix_timestamp(
                        (bundle.id.timestamp() / 1000) as i64,
                    )
                    .map_err(|_| {
                        error!(error = "Unable to parse timestamp");
                        s3_error!(InternalError, "Unable to parse timestamp")
                    })?
                    .into(),
                ),
                e_tag: Some(format!("-{}", bundle.id)),
                ..Default::default()
            }));
        }

        let (object, location) = objects_state.extract_object()?;

        let content_len = location.map(|l| l.raw_content_len).unwrap_or_default();

        let mime = mime_guess::from_path(object.name.as_str()).first();

        let output = HeadObjectOutput {
            content_length: Some(content_len),
            last_modified: Some(
                time::OffsetDateTime::from_unix_timestamp((object.id.timestamp() / 1000) as i64)
                    .map_err(|e| {
                        error!(error = ?e, msg = "Unable to parse timestamp");
                        s3_error!(InternalError, "Unable to parse timestamp")
                    })?
                    .into(),
            ),
            e_tag: Some(object.id.to_string()),
            content_disposition: Some(format!(r#"attachment;filename="{}""#, object.name)),
            content_type: mime,
            ..Default::default()
        };

        debug!(?output);
        debug!(?headers);

        let mut resp = S3Response::new(output);
        if let Some(headers) = headers {
            for (k, v) in headers {
                resp.headers.insert(
                    HeaderName::from_bytes(k.as_bytes()).map_err(|_| {
                        error!(error = "Unable to parse header name");
                        s3_error!(InternalError, "Unable to parse header name")
                    })?,
                    HeaderValue::from_str(&v).map_err(|_| {
                        error!(error = "Unable to parse header value");
                        s3_error!(InternalError, "Unable to parse header value")
                    })?,
                );
            }
        }

        Ok(resp)
    }

    #[tracing::instrument(err)]
    #[allow(clippy::blocks_in_conditions)]
    async fn list_objects(
        &self,
        req: S3Request<ListObjectsInput>,
    ) -> S3Result<S3Response<ListObjectsOutput>> {
        let CheckAccessResult { headers, .. } = req
            .extensions
            .get::<CheckAccessResult>()
            .cloned()
            .ok_or_else(|| {
                error!(error = "No context found");
                s3_error!(InternalError, "No context found")
            })?;
        // Fetch the project name, delimiter and prefix from the request
        let project_name = &req.input.bucket;
        let delimiter = req.input.delimiter;
        let prefix = req.input.prefix.filter(|prefix| !prefix.is_empty());

        // Check if bucket exists as root in cache of paths
        match self.cache.get_path(project_name.as_str()) {
            Some(_) => {}
            None => {
                error!("No bucket found");
                return Err(s3_error!(NoSuchBucket, "No bucket found"));
            }
        };

        let max_keys = match req.input.max_keys {
            Some(k) if k < 1000 => k as usize,
            _ => 1000usize,
        };

        let start_at = match &req.input.marker {
            Some(marker) => marker,
            None => "",
        };

        let (keys, common_prefixes, next_marker) = list_response(
            &self.cache,
            &delimiter,
            &prefix,
            project_name,
            start_at,
            max_keys,
            true,
        )
        .await
        .map_err(|_| {
            error!(error = "Keys not found in ListObjects");
            s3_error!(NoSuchKey, "Keys not found in ListObjects")
        })?;

        let common_prefixes = Some(
            common_prefixes
                .into_iter()
                .map(|e| CommonPrefix { prefix: Some(e) })
                .collect(),
        );
        let contents = Some(
            keys.into_iter()
                .map(|e| Object {
                    checksum_algorithm: None,
                    e_tag: Some(e.etag.to_string()),
                    key: Some(e.key),
                    last_modified: e.created_at.map(|t| {
                        s3s::dto::Timestamp::from(
                            time::OffsetDateTime::from_unix_timestamp(t.and_utc().timestamp())
                                .unwrap_or_else(|_| {
                                    error!(error = "Unable to parse timestamp");
                                    time::OffsetDateTime::now_utc()
                                }),
                        )
                    }),
                    owner: None,
                    size: Some(e.size),
                    ..Default::default()
                })
                .collect(),
        );

        let result = ListObjectsOutput {
            common_prefixes,
            contents,
            marker: req.input.marker,
            delimiter,
            encoding_type: None,
            is_truncated: Some(next_marker.is_some()),
            max_keys: Some(max_keys.try_into().map_err(|err| {
                error!(error = ?err, "Conversion failure");
                s3_error!(InternalError, "[BACKEND] Conversion failure: {}", err)
            })?),
            name: Some(project_name.clone()),
            next_marker,
            prefix,
            ..Default::default()
        };
        debug!(?result);

        let mut resp = S3Response::new(result);

        if let Some(headers) = headers {
            for (k, v) in headers {
                resp.headers.insert(
                    HeaderName::from_bytes(k.as_bytes()).map_err(|_| {
                        error!(error = "Unable to parse header name");
                        s3_error!(InternalError, "Unable to parse header name")
                    })?,
                    HeaderValue::from_str(&v).map_err(|_| {
                        error!(error = "Unable to parse header value");
                        s3_error!(InternalError, "Unable to parse header value")
                    })?,
                );
            }
        }

        Ok(resp)
    }

    #[tracing::instrument(err)]
    #[allow(clippy::blocks_in_conditions)]
    async fn list_objects_v2(
        &self,
        req: S3Request<ListObjectsV2Input>,
    ) -> S3Result<S3Response<ListObjectsV2Output>> {
        let CheckAccessResult { headers, .. } = req
            .extensions
            .get::<CheckAccessResult>()
            .cloned()
            .ok_or_else(|| {
                error!(error = "No context found");
                s3_error!(InternalError, "No context found")
            })?;
        // Fetch the project name, delimiter and prefix from the request
        let project_name = &req.input.bucket;
        let delimiter = req.input.delimiter;
        let prefix = req.input.prefix.filter(|prefix| !prefix.is_empty());

        // Check if bucket exists as root in cache of paths
        match self.cache.get_path(project_name.as_str()) {
            Some(_) => {}
            None => {
                error!("No bucket found");
                return Err(s3_error!(NoSuchBucket, "No bucket found"));
            }
        };

        // Process continuation token from request
        let continuation_token = match req.input.continuation_token {
            Some(t) => {
                let decoded_token = general_purpose::STANDARD_NO_PAD.decode(t).map_err(|_| {
                    error!(error = "Invalid continuation token");
                    s3_error!(InvalidToken, "Invalid continuation token")
                })?;
                let decoded_token = std::str::from_utf8(&decoded_token)
                    .map_err(|_| {
                        error!(error = "Invalid continuation token");
                        s3_error!(InvalidToken, "Invalid continuation token")
                    })?
                    .to_string();
                Some(decoded_token)
            }
            None => None,
        };

        // Filter all objects from cache which have the project as root
        let start_after = match (req.input.start_after, continuation_token.clone()) {
            (Some(_), Some(ct)) => ct,
            (None, Some(ct)) => ct,
            (Some(s), None) => s,
            _ => "".to_string(),
        };

        let max_keys = match req.input.max_keys {
            Some(k) if k < 1000 => k as usize,
            _ => 1000usize,
        };

        let (keys, common_prefixes, new_continuation_token) = list_response(
            &self.cache,
            &delimiter,
            &prefix,
            project_name,
            &start_after,
            max_keys,
            false,
        )
        .await
        .map_err(|_| {
            error!(error = "Keys not found in ListObjectsV2");
            s3_error!(NoSuchKey, "Keys not found in ListObjectsV2")
        })?;

        let key_count = (keys.len() + common_prefixes.len()) as i32;
        let common_prefixes = Some(
            common_prefixes
                .into_iter()
                .map(|e| CommonPrefix { prefix: Some(e) })
                .collect(),
        );
        let contents = Some(
            keys.into_iter()
                .map(|e| Object {
                    checksum_algorithm: None,
                    e_tag: Some(e.etag.to_string()),
                    key: Some(e.key),
                    last_modified: e.created_at.map(|t| {
                        s3s::dto::Timestamp::from(
                            time::OffsetDateTime::from_unix_timestamp(t.and_utc().timestamp())
                                .unwrap_or_else(|_| {
                                    error!(error = "Unable to parse timestamp");
                                    time::OffsetDateTime::now_utc()
                                }),
                        )
                    }),
                    owner: None,
                    size: Some(e.size),
                    ..Default::default()
                })
                .collect(),
        );

        let result = ListObjectsV2Output {
            common_prefixes,
            contents,
            continuation_token,
            delimiter,
            encoding_type: None,
            is_truncated: Some(new_continuation_token.is_some()),
            key_count: Some(key_count),
            max_keys: Some(max_keys.try_into().map_err(|err| {
                error!(error = ?err, "Conversion failure");
                s3_error!(InternalError, "[BACKEND] Conversion failure: {}", err)
            })?),
            name: Some(project_name.clone()),
            next_continuation_token: new_continuation_token,
            prefix,
            start_after: Some(start_after),
            ..Default::default()
        };
        debug!(?result);

        let mut resp = S3Response::new(result);

        if let Some(headers) = headers {
            for (k, v) in headers {
                resp.headers.insert(
                    HeaderName::from_bytes(k.as_bytes()).map_err(|_| {
                        error!(error = "Unable to parse header name");
                        s3_error!(InternalError, "Unable to parse header name")
                    })?,
                    HeaderValue::from_str(&v).map_err(|_| {
                        error!(error = "Unable to parse header value");
                        s3_error!(InternalError, "Unable to parse header value")
                    })?,
                );
            }
        }

        Ok(resp)
    }

    #[tracing::instrument(err)]
    #[allow(clippy::blocks_in_conditions)]
    async fn put_bucket_cors(
        &self,
        req: S3Request<PutBucketCorsInput>,
    ) -> S3Result<S3Response<PutBucketCorsOutput>> {
        let config = crate::structs::CORSConfiguration(
            req.input
                .cors_configuration
                .cors_rules
                .into_iter()
                .map(CORSRule::into)
                .collect(),
        );

        let data = req.extensions.get::<CheckAccessResult>().cloned();

        if let Some(client) = self.cache.aruna_client.read().await.as_ref() {
            let CheckAccessResult {
                objects_state,
                user_state,
                ..
            } = data.ok_or_else(|| {
                error!(error = "Missing data context");
                s3_error!(InvalidObjectState, "Missing CheckAccess extension")
            })?;

            let (object, _) = objects_state.require_regular()?;
            let bucket_obj = object.require_project()?;

            let token = user_state
                .sign_impersonating_token(self.cache.auth.read().await.as_ref())
                .ok_or_else(|| {
                    error!(error = "Unauthorized: Impersonating error");
                    s3_error!(NotSignedUp, "Unauthorized: Impersonating error")
                })?;

            client
                .add_or_replace_key_value_project(
                    &token,
                    bucket_obj.clone(),
                    Some((
                        "app.aruna-storage.org/cors",
                        &serde_json::to_string(&config).map_err(|_| {
                            error!(error = "Unable to serialize cors configuration");
                            s3_error!(InvalidArgument, "Unable to serialize cors configuration")
                        })?,
                    )),
                )
                .await
                .map_err(|_| {
                    error!(error = "Unable to update KeyValues");
                    s3_error!(InternalError, "Unable to update KeyValues")
                })?;
        }
        Ok(S3Response::new(PutBucketCorsOutput::default()))
    }

    #[tracing::instrument(err)]
    #[allow(clippy::blocks_in_conditions)]
    async fn delete_bucket_cors(
        &self,
        req: S3Request<DeleteBucketCorsInput>,
    ) -> S3Result<S3Response<DeleteBucketCorsOutput>> {
        let data = req.extensions.get::<CheckAccessResult>().cloned();

        if let Some(client) = self.cache.aruna_client.read().await.as_ref() {
            let CheckAccessResult {
                objects_state,
                user_state,
                ..
            } = data.ok_or_else(|| {
                error!(error = "Missing data context");
                s3_error!(InternalError, "Internal Error")
            })?;

            let (object, _) = objects_state.require_regular()?;
            let bucket_obj = object.require_project()?;

            let token = user_state
                .sign_impersonating_token(self.cache.auth.read().await.as_ref())
                .ok_or_else(|| {
                    error!(error = "Unauthorized: Impersonating error");
                    s3_error!(NotSignedUp, "Unauthorized: Impersonating error")
                })?;

            client
                .add_or_replace_key_value_project(&token, bucket_obj.clone(), None)
                .await
                .map_err(|_| {
                    error!(error = "Unable to update KeyValues");
                    s3_error!(InternalError, "Unable to update KeyValues")
                })?;
        }
        Ok(S3Response::new(DeleteBucketCorsOutput::default()))
    }

    #[tracing::instrument(err)]
    #[allow(clippy::blocks_in_conditions)]
    async fn get_bucket_location(
        &self,
        _req: S3Request<GetBucketLocationInput>,
    ) -> S3Result<S3Response<GetBucketLocationOutput>> {
        return Ok(S3Response::new(GetBucketLocationOutput {
            // TODO: Return proxy location / id -> Not possible restricted set of allowed locations
            location_constraint: None,
        }));
    }

    #[tracing::instrument(err)]
    #[allow(clippy::blocks_in_conditions)]
    async fn get_bucket_cors(
        &self,
        req: S3Request<GetBucketCorsInput>,
    ) -> S3Result<S3Response<GetBucketCorsOutput>> {
        let data = req.extensions.get::<CheckAccessResult>().cloned();

        let CheckAccessResult { objects_state, .. } = data.ok_or_else(|| {
            error!(error = "Missing data context");
            s3_error!(InvalidObjectState, "Missing CheckAccess extension")
        })?;

        let (object, _) = objects_state.require_regular()?;
        let bucket_obj = object.require_project()?;

        let cors = bucket_obj
            .key_values
            .clone()
            .into_iter()
            .find(|kv| kv.key == "app.aruna-storage.org/cors")
            .map(|kv| kv.value);

        if let Some(cors) = cors {
            let cors: crate::structs::CORSConfiguration =
                serde_json::from_str(&cors).map_err(|_| {
                    error!(error = "Unable to deserialize cors from JSON");
                    s3_error!(InvalidObjectState, "Unable to deserialize cors from JSON")
                })?;
            return Ok(S3Response::new(cors.into()));
        }
        Ok(S3Response::new(GetBucketCorsOutput::default()))
    }

    #[tracing::instrument(err)]
    #[allow(clippy::blocks_in_conditions)]
    async fn list_buckets(
        &self,
        req: S3Request<ListBucketsInput>,
    ) -> S3Result<S3Response<ListBucketsOutput>> {
        let data = req.extensions.get::<CheckAccessResult>().cloned();

        let CheckAccessResult { user_state, .. } = data.ok_or_else(|| {
            error!(error = "Missing data context");
            s3_error!(InvalidObjectState, "Missing CheckAccess extension")
        })?;

        return match user_state.get_user_id() {
            Some(user_id) => {
                let mut buckets = Vec::new();
                for o in self
                    .cache
                    .get_personal_projects(&user_id)
                    .await
                    .map_err(|_| {
                        error!(error = "Unable to get personal projects");
                        s3_error!(InternalError, "Unable to get personal projects")
                    })?
                {
                    buckets.push(Bucket {
                        creation_date: o.created_at.map(|t| {
                            s3s::dto::Timestamp::from(
                                time::OffsetDateTime::from_unix_timestamp(t.and_utc().timestamp())
                                    .unwrap(),
                            )
                        }),
                        name: Some(o.name),
                    });
                }
                let bs = if buckets.is_empty() {
                    None
                } else {
                    Some(buckets)
                };
                Ok(S3Response::new(ListBucketsOutput {
                    buckets: bs,
                    owner: Some(Owner {
                        display_name: None,
                        id: Some(user_id.to_string()),
                    }),
                }))
            }
            None => Err(s3_error!(InvalidAccessKeyId, "Invalid access key / user")),
        };
    }

    #[tracing::instrument(err)]
    #[allow(clippy::blocks_in_conditions)]
    async fn delete_object(
        &self,
        req: S3Request<DeleteObjectInput>,
    ) -> S3Result<S3Response<DeleteObjectOutput>> {
        let CheckAccessResult {
            objects_state,
            user_state,
            headers,
        } = req
            .extensions
            .get::<CheckAccessResult>()
            .cloned()
            .ok_or_else(|| {
                error!(error = "Missing data context");
                s3_error!(UnexpectedContent, "Missing data context")
            })?;

        let impersonating_token =
            user_state.sign_impersonating_token(self.cache.auth.read().await.as_ref());

        let (states, _) = objects_state.require_regular()?;

        let (_, _, _, object, _) = states.to_new_or_existing()?;

        match object {
            NewOrExistingObject::Missing(_) => {
                //Do nothing.
            }
            NewOrExistingObject::Existing(object) => {
                if let Some(handler) = self.cache.aruna_client.read().await.as_ref() {
                    if let Some(token) = &impersonating_token {
                        handler
                            .delete_object(&object.id, token)
                            .await
                            .map_err(|e| {
                                error!("Object deletion failed: {e}");
                                s3_error!(InternalError, "Object deletion failed")
                            })?;

                        self.cache.delete_object(object.id).await.map_err(|e| {
                            error!("Object cache deletion failed: {e}");
                            s3_error!(InternalError, "Object cache deletion failed")
                        })?;
                    }
                }
            }
            NewOrExistingObject::None => {
                return Err(s3_error!(
                    InvalidObjectState,
                    "Object in invalid state: None"
                ))
            }
        }

        let mut resp = S3Response::new(DeleteObjectOutput::default());
        if let Some(headers) = headers {
            for (k, v) in headers {
                resp.headers.insert(
                    HeaderName::from_bytes(k.as_bytes())
                        .map_err(|_| s3_error!(InternalError, "Unable to parse header name"))?,
                    HeaderValue::from_str(&v)
                        .map_err(|_| s3_error!(InternalError, "Unable to parse header value"))?,
                );
            }
        }
        Ok(resp)
    }

    #[tracing::instrument(err, skip(self, req))]
    #[allow(clippy::blocks_in_conditions)]
    async fn put_object(
        &self,
        req: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        let CheckAccessResult {
            objects_state,
            user_state,
            headers,
        } = req
            .extensions
            .get::<CheckAccessResult>()
            .cloned()
            .ok_or_else(|| {
                error!(error = "Missing data context");
                s3_error!(UnexpectedContent, "Missing data context")
            })?;

        let impersonating_token =
            user_state.sign_impersonating_token(self.cache.auth.read().await.as_ref());

        let (states, _) = objects_state.require_regular()?;

        let (_, collection, dataset, object, location_state) = states.to_new_or_existing()?;

        let (mut new_object, was_init) = match object {
            NewOrExistingObject::Existing(ob) => {
                if ob.object_status == Status::Initializing {
                    trace!("Object is initializing");
                    (ob, true)
                } else {
                    let mut new_revision = if let Some(handler) =
                        self.cache.aruna_client.read().await.as_ref()
                    {
                        if let Some(token) = &impersonating_token {
                            handler
                                .init_object_update(ob, token, true)
                                .await
                                .map_err(|_| {
                                    error!(error = "Object update failed");
                                    s3_error!(InternalError, "Object update failed")
                                })?
                        } else {
                            error!("missing impersonating token");
                            return Err(s3_error!(InternalError, "Token creation failed"));
                        }
                    } else {
                        error!("ArunaServer client not available");
                        return Err(s3_error!(InternalError, "ArunaServer client not available"));
                    };
                    new_revision.hashes = HashMap::default();
                    new_revision.synced = false;
                    new_revision.children = None;
                    new_revision.dynamic = false;
                    (new_revision, true)
                }
            }
            NewOrExistingObject::Missing(object) => (object, false),
            NewOrExistingObject::None => {
                return Err(s3_error!(
                    InvalidObjectState,
                    "Object in invalid state: None"
                ))
            }
        };

        trace!(?new_object);
        match req.input.content_length {
            Some(0) => {
                return self.put_empty_object(new_object, headers).await;
            }
            None => {
                error!("Missing content-length");
                return Err(s3_error!(MissingContentLength, "Missing content-length"));
            }
            _ => {} // Go on.
        };

        let mut location = self
            .backend
            .initialize_location(&new_object, req.input.content_length, location_state, false)
            .await
            .map_err(|_| {
                error!(error = "Unable to create object_location");
                s3_error!(InternalError, "Unable to create object_location")
            })?;
        trace!(?location);

        trace!("Initialized data location");

        // Initialize hashing transformers
        let (initial_sha_trans, initial_sha_recv) =
            HashingTransformer::new_with_backchannel(Sha256::new(), "sha256".to_string());
        let (initial_md5_trans, initial_md5_recv) =
            HashingTransformer::new_with_backchannel(Md5::new(), "md5".to_string());
        let (initial_size_trans, initial_size_recv) = SizeProbe::new();
        let (final_sha_trans, final_sha_recv) =
            HashingTransformer::new_with_backchannel(Sha256::new(), "sha256".to_string());
        let (final_size_trans, final_size_recv) = SizeProbe::new();

        match req.input.body {
            Some(data) => {
                let (tx, rx) = async_channel::bounded(10);

                let mut awr = GenericStreamReadWriter::new_with_sink(
                    data,
                    BufferedS3Sink::new(
                        self.backend.clone(),
                        location.clone(),
                        None,
                        None,
                        false,
                        None,
                        false,
                    )
                    .0,
                );

                awr.add_message_receiver(rx).await.map_err(|_| {
                    error!(error = "Internal notifier error");
                    s3_error!(InternalError, "Internal notifier error")
                })?;

                awr = awr.add_transformer(initial_sha_trans);
                awr = awr.add_transformer(initial_md5_trans);
                awr = awr.add_transformer(initial_size_trans);

                if location.is_compressed() && !location.is_pithos() {
                    trace!("adding zstd decompressor");
                    awr = awr.add_transformer(ZstdEnc::new());
                }

                if let Some(enc_key) = &location.get_encryption_key() {
                    if !location.is_pithos() {
                        awr = awr.add_transformer(ChaCha20Enc::new_with_fixed(*enc_key).map_err(
                            |_| {
                                error!(error = "Unable to initialize ChaCha20Enc");
                                s3_error!(
                                    InternalError,
                                    "Internal data transformer encryption error"
                                )
                            },
                        )?);
                    }
                }

                if location.is_pithos() {
                    let ctx = new_object
                        .get_file_context(Some(location.clone()), req.input.content_length)
                        .map_err(|_| {
                            error!(error = "Unable to get file context");
                            s3_error!(InternalError, "Unable to get file context")
                        })?;
                    tx.send(PithosMessage::FileContext(ctx))
                        .await
                        .map_err(|_| {
                            error!(error = "Internal notifier error");
                            s3_error!(InternalError, "Internal notifier error")
                        })?;
                    awr = awr.add_transformer(PithosTransformer::new());
                    awr = awr.add_transformer(FooterGenerator::new(None));
                }
                awr = awr.add_transformer(final_sha_trans);
                awr = awr.add_transformer(final_size_trans);

                awr.process().await.map_err(|_| {
                    error!(error = "Internal data transformer processing error");
                    s3_error!(InternalError, "Internal data transformer processing error")
                })?;
            }
            None => {
                error!("Empty body is not allowed");
                return Err(s3_error!(InvalidRequest, "Empty body is not allowed"));
            }
        }

        let mut collection_id = None;
        if let NewOrExistingObject::Missing(collection) = collection {
            if let Some(handler) = self.cache.aruna_client.read().await.as_ref() {
                if let Some(token) = &impersonating_token {
                    let col = handler
                        .create_collection(collection, token)
                        .await
                        .map_err(|_| {
                            error!(error = "Unable to create collection");
                            s3_error!(InternalError, "Unable to create collection")
                        })?;
                    collection_id = Some(col.id)
                }
            }
        }

        let mut dataset_id = None;
        if let NewOrExistingObject::Missing(mut dataset) = dataset {
            if let Some(handler) = self.cache.aruna_client.read().await.as_ref() {
                if let Some(token) = &impersonating_token {
                    if let Some(collection_id) = collection_id {
                        dataset.parents = Some(HashSet::from_iter([TypedRelation::Collection(
                            collection_id,
                        )]));
                    }
                    let dataset = handler.create_dataset(dataset, token).await.map_err(|_| {
                        error!(error = "Unable to create dataset");
                        s3_error!(InternalError, "Unable to create dataset")
                    })?;
                    dataset_id = Some(dataset.id);
                }
            }
        }

        if let Some(dataset_id) = dataset_id {
            new_object.parents = Some(HashSet::from_iter([TypedRelation::Dataset(dataset_id)]));
        } else if let Some(collection_id) = collection_id {
            new_object.parents = Some(HashSet::from_iter([TypedRelation::Collection(
                collection_id,
            )]));
        }

        // Fetch calculated hashes
        trace!("fetching hashes");
        let md5_initial = Some(initial_md5_recv.try_recv().map_err(|_| {
            error!(error = "Unable to md5 hash initial data");
            s3_error!(InternalError, "Unable to md5 hash initial data")
        })?);
        let sha_initial = Some(initial_sha_recv.try_recv().map_err(|_| {
            error!(error = "Unable to sha hash initial data");
            s3_error!(InternalError, "Unable to sha hash initial data")
        })?);
        let sha_final: String = final_sha_recv.try_recv().map_err(|_| {
            error!(error = "Unable to sha hash final data");
            s3_error!(InternalError, "Unable to sha hash final data")
        })?;
        let initial_size: u64 = initial_size_recv.try_recv().map_err(|_| {
            error!(error = "Unable to get size");
            s3_error!(InternalError, "Unable to get size")
        })?;
        let final_size: u64 = final_size_recv.try_recv().map_err(|_| {
            error!(error = "Unable to get size");
            s3_error!(InternalError, "Unable to get size")
        })?;

        new_object.hashes = vec![
            (
                "MD5".to_string(),
                md5_initial.clone().ok_or_else(|| {
                    error!(error = "Unable to get md5 hash initial data");
                    s3_error!(InternalError, "Unable to get md5 hash initial data")
                })?,
            ),
            (
                "SHA256".to_string(),
                sha_initial.clone().ok_or_else(|| {
                    error!(error = "Unable to get sha hash initial data");
                    s3_error!(InternalError, "Unable to get sha hash initial data")
                })?,
            ),
        ]
        .into_iter()
        .collect::<HashMap<String, String>>();

        let hashes = vec![
            Hash {
                alg: Hashalgorithm::Sha256.into(),
                hash: sha_initial.clone().ok_or_else(|| {
                    error!(error = "Unable to get sha hash initial data");
                    s3_error!(InternalError, "Unable to get sha hash initial data")
                })?,
            },
            Hash {
                alg: Hashalgorithm::Md5.into(),
                hash: md5_initial.clone().ok_or_else(|| {
                    error!(error = "Unable to get md5 hash initial data");
                    s3_error!(InternalError, "Unable to get md5 hash initial data")
                })?,
            },
        ];

        location.raw_content_len = initial_size as i64;
        location.disk_content_len = final_size as i64;
        location.disk_hash = Some(sha_final.clone());

        trace!("finishing object");
        if let Some(handler) = self.cache.aruna_client.read().await.as_ref() {
            if let Some(token) = &impersonating_token {
                if was_init {
                    new_object = handler
                        .finish_object(
                            new_object.id,
                            location.raw_content_len,
                            hashes.clone(),
                            token,
                        )
                        .await
                        .map_err(|_| {
                            error!(error = "Unable to finish object");
                            s3_error!(InternalError, "Unable to finish object")
                        })?;
                } else {
                    new_object = handler
                        .create_and_finish(new_object.clone(), location.raw_content_len, token)
                        .await
                        .map_err(|e| {
                            error!(error = ?e, "Unable to create and finish object");
                            s3_error!(InvalidObjectState, "{}", e.to_string())
                        })?;
                }
            }
        }

        self.cache
            .add_location_with_binding(new_object.id, location)
            .await
            .map_err(|e| {
                error!(error = ?e, msg = "Unable to add location with binding");
                s3_error!(InternalError, "Unable to add location with binding")
            })?;

        let output = PutObjectOutput {
            e_tag: md5_initial,
            checksum_sha256: sha_initial,
            version_id: Some(new_object.id.to_string()),
            ..Default::default()
        };
        debug!(?output);

        let mut resp = S3Response::new(output);
        if let Some(headers) = headers {
            for (k, v) in headers {
                resp.headers.insert(
                    HeaderName::from_bytes(k.as_bytes())
                        .map_err(|_| s3_error!(InternalError, "Unable to parse header name"))?,
                    HeaderValue::from_str(&v)
                        .map_err(|_| s3_error!(InternalError, "Unable to parse header value"))?,
                );
            }
        }

        Ok(resp)
    }

    #[tracing::instrument(err)]
    #[allow(clippy::blocks_in_conditions)]
    async fn upload_part(
        &self,
        req: S3Request<UploadPartInput>,
    ) -> S3Result<S3Response<UploadPartOutput>> {
        match req.input.content_length {
            Some(0) | None => {
                error!("Missing or invalid (0) content-length");
                return Err(s3_error!(
                    MissingContentLength,
                    "Missing or invalid (0) content-length"
                ));
            }
            Some(bytes) => {
                if bytes > 5 * 1024 * 1024 * 1024 {
                    error!("Content-Length exceeds 5GB");
                    return Err(s3_error!(EntityTooLarge, "Content-Length larger than 5Gib"));
                }
            }
        };

        let CheckAccessResult {
            objects_state,
            headers,
            ..
        } = req
            .extensions
            .get::<CheckAccessResult>()
            .cloned()
            .ok_or_else(|| {
                error!(error = "Missing data context");
                s3_error!(UnexpectedContent, "Missing data context")
            })?;

        // If the object exists and the signatures match -> Skip the download

        let (object, location) = objects_state.require_regular()?;
        let object = object.require_object()?;

        let location = location.ok_or_else(|| {
            error!(error = "Unable to get resource");
            s3_error!(NoSuchKey, "Object not found")
        })?;

        let etag = match req.input.body {
            Some(data) => {
                trace!("streaming data to backend");

                let (sink, receiver) = BufferedS3Sink::new(
                    self.backend.clone(),
                    location.clone(),
                    location.upload_id.clone(),
                    Some(req.input.part_number),
                    true,
                    None,
                    true,
                );

                let mut awr = GenericStreamReadWriter::new_with_sink(data, sink);

                let (before_probe, before_receiver) = SizeProbe::new();
                awr = awr.add_transformer(before_probe);

                let (after_probe, after_receiver) = SizeProbe::new();

                if let Some(enc_key) = &location.get_encryption_key() {
                    trace!("adding chacha20 encryption");
                    awr = awr.add_transformer(ChaCha20Enc::new_with_fixed(*enc_key).map_err(
                        |_| {
                            error!(error = "Unable to initialize ChaCha20Enc");
                            s3_error!(InternalError, "Internal data transformer encryption error")
                        },
                    )?);
                }

                awr = awr.add_transformer(after_probe);

                awr.process().await.map_err(|_| {
                    error!(error = "Internal data transformer processing error");
                    s3_error!(InternalError, "Internal data transformer processing error")
                })?;

                let before_size = before_receiver.try_recv().map_err(|_| {
                    error!(error = "Unable to get size");
                    s3_error!(InternalError, "Unable to get size")
                })?;

                let after_size = after_receiver.try_recv().map_err(|_| {
                    error!(error = "Unable to get size");
                    s3_error!(InternalError, "Unable to get size")
                })?;

                self.cache
                    .create_multipart_upload(
                        location.upload_id.ok_or_else(|| {
                            error!(error = "Unable to get upload_id");
                            s3_error!(InternalError, "Unable to get upload_id")
                        })?,
                        object.id,
                        req.input.part_number as u64,
                        before_size,
                        after_size,
                    )
                    .await
                    .map_err(|_| {
                        error!(error = "Unable to create multipart upload");
                        s3_error!(InternalError, "Unable to create multipart upload")
                    })?;

                if let Some(r) = receiver {
                    r.recv().await.map_err(|_| {
                        error!(error = "Unable to query etag");
                        s3_error!(InternalError, "Unable to query etag")
                    })?
                } else {
                    error!("receiver is none");
                    return Err(s3_error!(InternalError, "receiver is none"));
                }
            }
            None => {
                error!("empty body is not allowed");
                return Err(s3_error!(InvalidRequest, "Empty body is not allowed"));
            }
        };

        let output = UploadPartOutput {
            e_tag: Some(format!("-{etag}")),
            ..Default::default()
        };
        debug!(?output);

        let mut resp = S3Response::new(output);
        if let Some(headers) = headers {
            for (k, v) in headers {
                resp.headers.insert(
                    HeaderName::from_bytes(k.as_bytes())
                        .map_err(|_| s3_error!(InternalError, "Unable to parse header name"))?,
                    HeaderValue::from_str(&v)
                        .map_err(|_| s3_error!(InternalError, "Unable to parse header value"))?,
                );
            }
        }

        Ok(resp)
    }
}
