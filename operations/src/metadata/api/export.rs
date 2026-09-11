use super::*;

pub async fn get_visible_metadata_document(
    context: &DriverContext,
    realm_id: RealmId,
    request: GetVisibleMetadataDocumentRequest,
) -> Result<MetadataRegistryRecord, MetadataApiError> {
    let record = load_record_by_document(context, request.document_id).await?;
    ensure_record_readable(context, realm_id, request.auth.as_ref(), &record, None).await?;
    ensure_record_materialized_for_graph_read(context, &record).await?;
    Ok(record)
}

pub async fn export_metadata_rocrate(
    context: &DriverContext,
    realm_id: RealmId,
    request: ExportMetadataRoCrateRequest,
) -> Result<ExportMetadataRoCrateResult, MetadataApiError> {
    if request.view == MetadataRoCrateExportView::Raw {
        return export_raw(context, realm_id, request).await;
    }
    let record = load_record_by_document(context, request.document_id).await?;
    ensure_record_readable(context, realm_id, request.auth.as_ref(), &record, None).await?;

    match request.view {
        MetadataRoCrateExportView::Full => {
            ensure_record_materialized_for_graph_read(context, &record).await?;
            Ok(ExportMetadataRoCrateResult::Full {
                jsonld: export_rocrate_jsonld(context, &record.graph_iri).await?,
                record,
            })
        }
        MetadataRoCrateExportView::Summary => {
            ensure_record_materialized_for_graph_read(context, &record).await?;
            Ok(ExportMetadataRoCrateResult::Summary {
                jsonld: export_rocrate_summary_jsonld(
                    context,
                    &record.graph_iri,
                    record.last_event_id,
                )
                .await?,
                record,
            })
        }
        MetadataRoCrateExportView::Page => {
            ensure_record_materialized_for_graph_read(context, &record).await?;
            Ok(ExportMetadataRoCrateResult::Page {
                page: export_rocrate_page(
                    context,
                    &record.graph_iri,
                    request.limit,
                    request.offset,
                    request.after,
                )
                .await?,
                record,
            })
        }
        MetadataRoCrateExportView::Raw => Err(MetadataApiError::Internal(
            "raw export snapshot dispatch mismatch".to_string(),
        )),
    }
}

pub(super) async fn export_raw(
    context: &DriverContext,
    realm_id: RealmId,
    request: ExportMetadataRoCrateRequest,
) -> Result<ExportMetadataRoCrateResult, MetadataApiError> {
    let mut owner = context
        .storage_handle
        .start_transaction(true)
        .await
        .map_err(|error| MetadataApiError::Internal(error.to_string()))?;
    let txn_id = owner.id().ok_or_else(|| {
        MetadataApiError::Internal("read snapshot owner missing transaction".to_string())
    })?;
    let result = export_raw_txn(context, realm_id, &request, txn_id).await;
    match result {
        Ok(result) => {
            owner.unknown();
            match context
                .storage_handle
                .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
                .await
            {
                Event::Storage(StorageEvent::TransactionCommitted { .. }) => {
                    owner.finish();
                    Ok(result)
                }
                Event::Storage(StorageEvent::Error { error }) => {
                    Err(MetadataApiError::Internal(error.to_string()))
                }
                other => Err(MetadataApiError::Internal(format!(
                    "unexpected read snapshot commit event: {other:?}"
                ))),
            }
        }
        Err(error) => {
            match context
                .storage_handle
                .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                .await
            {
                Event::Storage(StorageEvent::TransactionAborted { .. })
                | Event::Storage(StorageEvent::Error {
                    error: aruna_core::errors::StorageError::TransactionNotFound,
                }) => owner.finish(),
                _ => {}
            }
            Err(error)
        }
    }
}

pub(super) fn raw_identity_matches(
    record: &MetadataRegistryRecord,
    realm_id: RealmId,
    document_id: Ulid,
) -> bool {
    record.realm_id == realm_id && record.document_id == document_id
}

pub(super) async fn export_raw_txn(
    context: &DriverContext,
    realm_id: RealmId,
    request: &ExportMetadataRoCrateRequest,
    txn_id: TxnId,
) -> Result<ExportMetadataRoCrateResult, MetadataApiError> {
    let record = load_record_txn(context, request.document_id, txn_id).await?;
    if !raw_identity_matches(&record, realm_id, request.document_id) {
        return Err(MetadataApiError::NotFound);
    }
    ensure_record_readable(
        context,
        realm_id,
        request.auth.as_ref(),
        &record,
        Some(txn_id),
    )
    .await?;
    let raw =
        crate::metadata::raw_revision::load_raw_view(context, record.document_id, Some(txn_id))
            .await
            .map_err(|error| match error {
                crate::metadata::raw_revision::MetadataRawReadError::LimitExceeded(_) => {
                    MetadataApiError::ServiceUnavailable
                }
                error => MetadataApiError::Internal(error.to_string()),
            })?
            .ok_or(MetadataApiError::NotFound)?;
    let dataset_digest = raw.revision.dataset_digest;
    Ok(ExportMetadataRoCrateResult::Raw {
        record,
        raw,
        dataset_digest,
    })
}
