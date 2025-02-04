use crate::caching::cache::Cache;
use crate::data_backends::storage_backend::StorageBackend;
use crate::s3_frontend::utils::buffered_s3_sink::BufferedS3Sink;
use crate::structs::Object;
use crate::structs::ObjectLocation;
use anyhow::anyhow;
use anyhow::Result;
use aruna_rust_api::api::storage::models::v2::Hash;
use aruna_rust_api::api::storage::models::v2::Hashalgorithm;
use aruna_rust_api::api::storage::models::v2::Status;
use diesel_ulid::DieselUlid;
use md5::{Digest, Md5};
use pithos_lib::streamreadwrite::GenericStreamReadWriter;
use pithos_lib::transformer::ReadWriter;
use pithos_lib::transformers::decrypt_with_parts::ChaCha20DecParts;
use pithos_lib::transformers::encrypt::ChaCha20Enc;
use pithos_lib::transformers::footer::FooterGenerator;
use pithos_lib::transformers::hashing_transformer::HashingTransformer;
use pithos_lib::transformers::pithos_comp_enc::PithosTransformer;
use pithos_lib::transformers::size_probe::SizeProbe;
use pithos_lib::transformers::zstd_comp::ZstdEnc;
use pithos_lib::transformers::zstd_decomp::ZstdDec;
use sha2::Sha256;
use std::fmt::Debug;
use std::sync::Arc;
use tokio::pin;
use tracing::debug;
use tracing::error;
use tracing::info_span;
use tracing::trace;
use tracing::Instrument;

#[derive(Debug)]
pub struct DataHandler {}

impl DataHandler {
    #[tracing::instrument(
        level = "trace",
        skip(object, cache, backend, before_location, path_level)
    )]
    pub async fn finalize_location(
        object: Object,
        cache: Arc<Cache>,
        backend: Arc<Box<dyn StorageBackend>>,
        before_location: ObjectLocation,
        path_level: Option<[Option<(DieselUlid, String)>; 4]>,
    ) -> Result<()> {
        let token = if let Some(handler) = cache.auth.read().await.as_ref() {
            let Some(created_by) = object.created_by else {
                error!("No created_by found");
                return Err(anyhow!("No created_by found"));
            };
            handler
                .sign_impersonating_token(created_by.to_string(), None::<String>)
                .map_err(|e| {
                    error!(error = ?e, msg = e.to_string());
                    e
                })?
        } else {
            error!("No handler found");
            return Err(anyhow!("No handler found"));
        };

        let upload_id = before_location
            .upload_id
            .as_ref()
            .ok_or_else(|| anyhow!("Missing upload_id"))?
            .to_string();

        let parents = if let Some(levels) = path_level {
            levels
        } else {
            cache.get_single_parent(&object.id).await.map_err(|e| {
                error!(error = ?e, msg = e.to_string());
                e
            })?
        };

        let mut new_location = backend
            .initialize_location(&object, None, parents, false)
            .await?;

        debug!(?before_location, ?new_location, "Finalizing location");

        let ctx = object.get_file_context(
            Some(new_location.clone()),
            Some(before_location.disk_content_len),
        )?;

        let (tx_send, tx_receive) = async_channel::bounded(10);

        let clone_key = before_location.get_encryption_key();

        let before_location = before_location.clone();
        let backend_clone = backend.clone();
        let new_location_clone = new_location.clone();
        let is_compressed = before_location.file_format.is_compressed();

        let mut part_lens = Vec::new();
        let parts = cache.get_parts(&upload_id);
        for part in parts {
            let full_chunks = (part.size / (65536 + 28)) * (65536 + 28);
            if full_chunks != 0 {
                part_lens.push(full_chunks);
            }
            if part.size % (65536 + 28) != 0 {
                part_lens.push(part.size - full_chunks);
            }
        }

        trace!(part_lens = ?part_lens, "Part lengths");

        let aswr_handle = tokio::spawn(
            async move {
                let (tx, rx) = async_channel::bounded(10);
                let (sink, _) = BufferedS3Sink::new(
                    backend_clone,
                    new_location_clone.clone(),
                    None,
                    None,
                    false,
                    None,
                    false,
                );

                pin!(tx_receive);
                // Bind to variable to extend the lifetime of arsw to the end of the function
                let mut asr = GenericStreamReadWriter::new_with_sink(tx_receive, sink);

                asr.add_message_receiver(rx).await?;

                tracing::debug!(part_lens = ?part_lens, "Part lengths");

                if let Some(key) = clone_key {
                    asr = asr.add_transformer(ChaCha20DecParts::new_with_lengths(key, part_lens));
                }

                if is_compressed {
                    asr = asr.add_transformer(ZstdDec::new());
                }

                let (uncompressed_probe, uncompressed_stream) = SizeProbe::new();

                asr = asr.add_transformer(uncompressed_probe);

                let (sha_transformer, sha_recv) =
                    HashingTransformer::new_with_backchannel(Sha256::new(), "sha256".to_string());
                let (md5_transformer, md5_recv) =
                    HashingTransformer::new_with_backchannel(Md5::new(), "md5".to_string());

                asr = asr.add_transformer(sha_transformer);
                asr = asr.add_transformer(md5_transformer);

                if new_location_clone.is_compressed() && !new_location_clone.is_pithos() {
                    trace!("adding zstd decompressor");
                    asr = asr.add_transformer(ZstdEnc::new());
                }

                if let Some(enc_key) = &new_location_clone.get_encryption_key() {
                    if !new_location_clone.is_pithos() {
                        asr = asr.add_transformer(ChaCha20Enc::new_with_fixed(*enc_key).map_err(
                            |e| {
                                error!(error = ?e, msg = "Unable to initialize ChaCha20Enc");
                                e
                            },
                        )?);
                    }
                }

                if new_location_clone.is_pithos() {
                    tx.send(pithos_lib::helpers::notifications::Message::FileContext(
                        ctx,
                    ))
                    .await?;
                    asr = asr.add_transformer(PithosTransformer::new());
                    asr = asr.add_transformer(FooterGenerator::new(None));
                }

                let (final_sha, final_sha_recv) =
                    HashingTransformer::new_with_backchannel(Sha256::new(), "sha256".to_string());

                asr = asr.add_transformer(final_sha);

                let (disk_size_probe, disk_size_stream) = SizeProbe::new();
                asr = asr.add_transformer(disk_size_probe);

                asr.process().await.map_err(|e| {
                    error!(error = ?e, msg = e.to_string());
                    e
                })?;

                Ok::<(u64, u64, String, String, String), anyhow::Error>((
                    disk_size_stream.try_recv().inspect_err(|&e| {
                        error!(error = ?e, msg = e.to_string());
                    })?,
                    uncompressed_stream.try_recv().inspect_err(|&e| {
                        error!(error = ?e, msg = e.to_string());
                    })?,
                    sha_recv.try_recv().inspect_err(|&e| {
                        error!(error = ?e, msg = e.to_string());
                    })?,
                    md5_recv.try_recv().inspect_err(|&e| {
                        error!(error = ?e, msg = e.to_string());
                    })?,
                    final_sha_recv.try_recv().inspect_err(|&e| {
                        error!(error = ?e, msg = e.to_string());
                    })?,
                ))
            }
            .instrument(info_span!("finalize_location")),
        );

        match backend
            .get_object(before_location.clone(), None, tx_send)
            .await{
            Ok(_) => {}
            Err(e) => {
                let object_id = object.id;
                error!(location = ?before_location, object_id = ?object_id, error = ?e, msg = "Failed to get multipart for location");
                if object.object_status == Status::Initializing {
                    let upload_id = before_location
                        .upload_id
                        .as_ref()
                        .ok_or_else(|| anyhow!("Missing upload_id"))?
                        .to_string();
                    cache.delete_parts_by_upload_id(upload_id).await?;
                    cache.delete_location_with_mappings(object_id, before_location).await?;
                }else{
                    error!(?object_id, "Object is not in initializing state");
                }
                return Err(e);
            }
            }

        let (before_size, after_size, sha, md5, final_sha) = aswr_handle
            .await
            .map_err(|e| {
                error!(error = ?e, msg = e.to_string());
                e
            })?
            .map_err(|e| {
                error!(error = ?e, msg = e.to_string());
                e
            })?;

        new_location.disk_content_len = before_size as i64;
        new_location.raw_content_len = after_size as i64;
        new_location.disk_hash = Some(final_sha);

        debug!(new_location = ?new_location, "Finished finalizing location");

        let hashes = vec![
            Hash {
                alg: Hashalgorithm::Sha256.into(),
                hash: sha,
            },
            Hash {
                alg: Hashalgorithm::Md5.into(),
                hash: md5,
            },
        ];

        if let Some(handler) = cache.aruna_client.read().await.as_ref() {
            // Set id of new location to object id to satisfy FK constraint
            // TODO: Update hashes etc.

            handler
                .set_object_hashes(&object.id, hashes, &token)
                .await?;

            cache.update_location(object.id, new_location).await?;

            let upload_id = before_location
                .upload_id
                .as_ref()
                .ok_or_else(|| anyhow!("Missing upload_id"))?
                .to_string();
            backend.delete_object(before_location).await?;

            cache.delete_parts_by_upload_id(upload_id).await?;
        }

        Ok(())
    }
}
