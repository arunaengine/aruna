use super::*;

#[test]
fn config_round_trips() {
    // The transport form must preserve every planner and quota value: a
    // dropped link would silently change every transfer estimate.
    let stored = RealmComputeConfig {
        links: vec![LocationLink {
            from: "eu-west".to_string(),
            to: "us-east".to_string(),
            bandwidth_bytes_per_sec: 125_000_000,
        }],
        group_quotas: vec![GroupComputeQuota {
            group_id: Ulid::from_bytes([7u8; 16]),
            quota: ComputeQuota {
                max_jobs: Some(4),
                max_job_walltime_ms: Some(1_000),
                ..ComputeQuota::default()
            },
        }],
        ..RealmComputeConfig::default()
    };

    let parsed = compute_config(config_body(&stored)).expect("body parses");

    assert_eq!(parsed, stored);
}

#[test]
fn conflict_is_retryable() {
    // A concurrent realm-configuration update must read as a retryable
    // conflict, not as a rejected configuration.
    use aruna_core::errors::StorageError;

    let conflict = map_compute_error(SetComputeError::StorageError(
        StorageError::TransactionConflict,
    ));
    assert!(matches!(conflict, ServerError::Conflict(_)));
    assert!(matches!(
        map_compute_error(SetComputeError::RealmConfigNotFound),
        ServerError::NotFound
    ));
    assert!(matches!(
        map_compute_error(SetComputeError::InvalidCompute {
            reason: "zero bandwidth".to_string()
        }),
        ServerError::BadRequestReason(_)
    ));
}

#[test]
fn capacity_is_unavailable() {
    // Cleanup capacity is transient, so it must not read as an internal error.
    use aruna_core::errors::StorageError;

    assert!(matches!(
        map_compute_error(SetComputeError::StorageError(StorageError::CleanupCapacity)),
        ServerError::ServiceUnavailableReason(_)
    ));
}

#[test]
fn rejects_group_id() {
    let mut body = config_body(&RealmComputeConfig::default());
    body.group_quotas.push(GroupQuotaBody {
        group_id: "not-a-ulid".to_string(),
        quota: ComputeQuotaBody::default(),
    });
    assert!(compute_config(body).is_err());
}
