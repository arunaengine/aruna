use aruna_core::errors::ConversionError;
use aruna_core::keyspaces::ONBOARDING_KEYSPACE;
use aruna_core::onboarding::{
    OnboardingSecretRecord, OnboardingSecretState, OnboardingSecretStateRecord,
};
use aruna_core::types::{Key, Value};
use byteview::ByteView;
use ulid::Ulid;

pub fn secret_state_key(enrollment_id: Ulid) -> ByteView {
    ByteView::from(format!("secret-state:{enrollment_id}").into_bytes())
}

pub fn secret_state_write_entry(
    enrollment_id: Ulid,
    state: OnboardingSecretState,
) -> Result<(String, Key, Value), ConversionError> {
    let record = OnboardingSecretStateRecord {
        enrollment_id,
        state,
    };
    Ok((
        ONBOARDING_KEYSPACE.to_string(),
        secret_state_key(enrollment_id),
        ByteView::from(postcard::to_allocvec(&record)?),
    ))
}

pub fn resolve_secret_state(
    record: &OnboardingSecretRecord,
    value: Option<&Value>,
) -> Result<OnboardingSecretState, ConversionError> {
    match value {
        Some(value) => {
            let state_record: OnboardingSecretStateRecord = postcard::from_bytes(value)?;
            if state_record.enrollment_id != record.enrollment_id {
                return Err(ConversionError::InvalidOperationConversion(format!(
                    "onboarding secret state enrollment {} does not match record {}",
                    state_record.enrollment_id, record.enrollment_id
                )));
            }
            Ok(state_record.state)
        }
        None => Err(ConversionError::InvalidOperationConversion(format!(
            "missing onboarding secret state for {}",
            record.enrollment_id
        ))),
    }
}
