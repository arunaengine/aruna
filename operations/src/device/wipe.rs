//! Checks a device wipe request and makes storage durable before the process erases its data.
//! Realm-side eviction of the device is a separate, earlier step.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::operation::Operation;
use aruna_core::rows::done;
use thiserror::Error;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WipeDeviceConfig {
    pub node_id: NodeId,
    /// The node id the caller typed back, so a wipe cannot be a stray click.
    pub confirm_node_id: String,
}

#[derive(Debug, PartialEq)]
pub struct WipeDeviceOperation {
    config: WipeDeviceConfig,
}

#[derive(Debug, Error, PartialEq)]
pub enum WipeDeviceError {
    #[error(transparent)]
    Io(#[from] IoError),
    #[error("the confirmation does not name this node")]
    ConfirmationMismatch,
}

impl WipeDeviceOperation {
    pub fn new(config: WipeDeviceConfig) -> Self {
        Self { config }
    }
}

impl Operation for WipeDeviceOperation {
    type Output = NodeId;
    type Error = WipeDeviceError;

    fn first(&mut self) -> Result<Next<NodeId>, WipeDeviceError> {
        if self.config.confirm_node_id != self.config.node_id.to_string() {
            return Err(WipeDeviceError::ConfirmationMismatch);
        }
        // Everything accepted so far is made durable before the tree goes, so
        // an interrupted wipe leaves a consistent store rather than a torn one.
        Ok(Next::Call(IoCall::Sync))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<NodeId>, WipeDeviceError> {
        done(result)?;
        Ok(Next::Done(self.config.node_id))
    }

    fn expected_error(error: &Self::Error) -> bool {
        matches!(error, WipeDeviceError::ConfirmationMismatch)
    }
}

#[cfg(test)]
mod pure_tests {
    use super::{WipeDeviceConfig, WipeDeviceError, WipeDeviceOperation};
    use aruna_core::calls::{IoCall, IoError, IoResult, Next};
    use aruna_core::operation::Operation;

    fn node() -> aruna_core::NodeId {
        iroh::SecretKey::from_bytes(&[8u8; 32]).public()
    }

    fn wipe(confirm_node_id: String) -> WipeDeviceOperation {
        WipeDeviceOperation::new(WipeDeviceConfig {
            node_id: node(),
            confirm_node_id,
        })
    }

    #[test]
    fn refuses_wrong_confirmation() {
        let mut operation = wipe("another-node".to_string());
        assert_eq!(
            operation.first(),
            Err(WipeDeviceError::ConfirmationMismatch)
        );
    }

    #[test]
    fn syncs_before_finishing() {
        let mut operation = wipe(node().to_string());
        assert_eq!(operation.first(), Ok(Next::Call(IoCall::Sync)));
        assert_eq!(operation.next(IoResult::Done), Ok(Next::Done(node())));
    }

    #[test]
    fn rejects_wrong_result() {
        // A durability barrier that never confirmed must not read as wiped.
        let mut operation = wipe(node().to_string());
        operation.first().unwrap();
        assert!(matches!(
            operation.next(IoResult::Woke),
            Err(WipeDeviceError::Io(IoError::Unexpected { .. }))
        ));
    }
}
