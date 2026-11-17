//! Checks a device wipe request and makes storage durable before the process erases its data.
//! Realm-side eviction of the device is a separate, earlier step.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::operation::Operation;
use aruna_core::types::Effects;
use smallvec::smallvec;
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
    state: WipeDeviceState,
    output: Option<Result<NodeId, WipeDeviceError>>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum WipeDeviceState {
    Init,
    SyncStorage,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum WipeDeviceError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error("the confirmation does not name this node")]
    ConfirmationMismatch,
    #[error("wiping the device did not finish")]
    NotFinished,
    #[error("unexpected event in state {state}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: &'static str,
    },
}

impl WipeDeviceOperation {
    pub fn new(config: WipeDeviceConfig) -> Self {
        Self {
            config,
            state: WipeDeviceState::Init,
            output: None,
        }
    }
}

impl Operation for WipeDeviceOperation {
    type Output = NodeId;
    type Error = WipeDeviceError;

    fn start(&mut self) -> Effects {
        if self.config.confirm_node_id != self.config.node_id.to_string() {
            return fail(self, WipeDeviceError::ConfirmationMismatch);
        }
        // Everything accepted so far is made durable before the tree goes, so
        // an interrupted wipe leaves a consistent store rather than a torn one.
        self.state = WipeDeviceState::SyncStorage;
        smallvec![Effect::Storage(StorageEffect::SyncAll)]
    }

    fn step(&mut self, event: Event) -> Effects {
        let event = match event {
            Event::Storage(StorageEvent::Error { error }) => {
                return fail(self, WipeDeviceError::StorageError(error));
            }
            other => other,
        };

        match self.state {
            WipeDeviceState::SyncStorage => {
                let got = event.label();
                let Event::Storage(StorageEvent::SyncAllFinished) = event else {
                    return fail(
                        self,
                        WipeDeviceError::UnexpectedEvent {
                            state: format!("{:?}", self.state),
                            expected: "sync all finished",
                            got,
                        },
                    );
                };
                self.state = WipeDeviceState::Finish;
                self.output = Some(Ok(self.config.node_id));
                smallvec![]
            }
            WipeDeviceState::Init | WipeDeviceState::Finish | WipeDeviceState::Error => {
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, WipeDeviceState::Finish | WipeDeviceState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(WipeDeviceError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }

    fn expected_error(error: &Self::Error) -> bool {
        matches!(error, WipeDeviceError::ConfirmationMismatch)
    }
}

fn fail(operation: &mut WipeDeviceOperation, error: WipeDeviceError) -> Effects {
    operation.state = WipeDeviceState::Error;
    operation.output = Some(Err(error));
    smallvec![]
}

#[cfg(test)]
mod pure_tests {
    use super::{WipeDeviceConfig, WipeDeviceError, WipeDeviceOperation, WipeDeviceState};
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::operation::Operation;

    fn node() -> aruna_core::NodeId {
        iroh::SecretKey::from_bytes(&[8u8; 32]).public()
    }

    #[test]
    fn refuses_wrong_confirmation() {
        let mut operation = WipeDeviceOperation::new(WipeDeviceConfig {
            node_id: node(),
            confirm_node_id: "another-node".to_string(),
        });
        assert!(operation.start().is_empty());
        assert!(operation.is_complete());
        assert_eq!(
            operation.finalize(),
            Err(WipeDeviceError::ConfirmationMismatch)
        );
    }

    #[test]
    fn syncs_before_finishing() {
        let node_id = node();
        let mut operation = WipeDeviceOperation::new(WipeDeviceConfig {
            node_id,
            confirm_node_id: node_id.to_string(),
        });
        let effects = operation.start();
        assert_eq!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::SyncAll)].as_slice()
        );
        assert!(!operation.is_complete());
        operation.step(Event::Storage(StorageEvent::SyncAllFinished));
        assert_eq!(operation.finalize(), Ok(node_id));
    }

    #[test]
    fn rejects_wrong_event() {
        // A durability barrier that never confirmed must not read as wiped.
        let node_id = node();
        let mut operation = WipeDeviceOperation::new(WipeDeviceConfig {
            node_id,
            confirm_node_id: node_id.to_string(),
        });
        operation.start();
        operation.step(Event::Storage(StorageEvent::TransactionAborted {
            txn_id: ulid::Ulid::from_parts(1, 1),
        }));
        assert_eq!(operation.state, WipeDeviceState::Error);
        assert!(matches!(
            operation.finalize(),
            Err(WipeDeviceError::UnexpectedEvent { .. })
        ));
    }
}
