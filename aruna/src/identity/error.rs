//! Errors of the persisted identity record and its enrollment transport.

use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::onboarding::{OnboardingMode, OnboardingSecretError};
use std::array::TryFromSliceError;
use std::string::FromUtf8Error;
use thiserror::Error;

/// A failure while reading, persisting, or enrolling a node identity.
#[derive(Error, Debug)]
pub enum IdentityError {
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error(transparent)]
    FromSliceError(#[from] TryFromSliceError),
    #[error(transparent)]
    Base64Error(#[from] base64::DecodeError),
    #[error(transparent)]
    SPKIError(#[from] ed25519_dalek::pkcs8::spki::Error),
    #[error(transparent)]
    PKCSError(#[from] ed25519_dalek::pkcs8::Error),
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    OnboardingSecretError(#[from] OnboardingSecretError),
    #[error(transparent)]
    ReqwestError(#[from] reqwest::Error),
    #[error(transparent)]
    Utf8Error(#[from] FromUtf8Error),
    #[error("onboarding bootstrap failed: {0}")]
    OnboardingBootstrapFailed(String),
    #[error("missing onboarding bootstrap material for {0:?} node")]
    MissingOnboardingMaterial(OnboardingMode),
    #[error("onboarding mode mismatch between secret and bootstrap response")]
    OnboardingModeMismatch,
    #[error("unexpected storage event while loading node state: {0}")]
    UnexpectedStorageEvent(String),
}
