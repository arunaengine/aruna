//! Owner-driven wipe of the device this node runs on.
//!
//! Realm-side eviction is a separate, earlier step: the desktop calls
//! `DELETE /users/me/devices/{id}` on a management node so the realm drops the
//! membership, then asks this node to erase what it holds locally.

use std::fs;
use std::path::{Path, PathBuf};

use aruna_core::NodeId;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::operation::Operation;
use aruna_core::types::Effects;
use smallvec::smallvec;
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use tracing::warn;

/// Exit status of a wiped node, so a supervisor tells an erased device apart
/// from a crash (1) and from an ordinary stop (0, 130, 143).
pub const WIPED_EXIT_CODE: i32 = 79;

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

#[derive(Clone, Debug, PartialEq)]
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
        got: String,
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

        match self.state.clone() {
            WipeDeviceState::SyncStorage => {
                let got = format!("{event:?}");
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

/// The armed wipe and the roots it erases. The device surface arms it and the
/// process erases the roots after the ordinary shutdown sequence, because the
/// stores keep their files open until then.
#[derive(Debug)]
pub struct DeviceWipe {
    roots: Vec<PathBuf>,
    armed: CancellationToken,
}

impl DeviceWipe {
    pub fn new(roots: Vec<PathBuf>) -> Self {
        Self {
            roots,
            armed: CancellationToken::new(),
        }
    }

    pub fn arm(&self) {
        self.armed.cancel();
    }

    pub fn is_armed(&self) -> bool {
        self.armed.is_cancelled()
    }

    /// Resolves once the wipe is armed.
    pub async fn wait(&self) {
        self.armed.cancelled().await;
    }

    pub fn roots(&self) -> &[PathBuf] {
        &self.roots
    }
}

/// Erases the contents of each root, keeping the roots themselves so a mounted
/// volume stays mounted. Answers with the entries it could not remove.
pub fn purge(roots: &[PathBuf]) -> Vec<PathBuf> {
    let mut failed = Vec::new();
    for root in roots {
        purge_root(root, &mut failed);
    }
    failed
}

fn purge_root(root: &Path, failed: &mut Vec<PathBuf>) {
    let entries = match fs::read_dir(root) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return,
        Err(error) => {
            warn!(error = %error, root = %root.display(), "Failed to read a wipe root");
            failed.push(root.to_path_buf());
            return;
        }
    };
    for entry in entries {
        let Ok(entry) = entry else {
            failed.push(root.to_path_buf());
            continue;
        };
        let path = entry.path();
        let removed = if path.is_dir() {
            fs::remove_dir_all(&path)
        } else {
            fs::remove_file(&path)
        };
        if let Err(error) = removed {
            warn!(error = %error, path = %path.display(), "Failed to erase a wiped path");
            failed.push(path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        DeviceWipe, WipeDeviceConfig, WipeDeviceError, WipeDeviceOperation, WipeDeviceState, purge,
    };
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::operation::Operation;
    use std::fs;
    use tempfile::tempdir;

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
            txn_id: ulid::Ulid::generate(),
        }));
        assert_eq!(operation.state, WipeDeviceState::Error);
        assert!(matches!(
            operation.finalize(),
            Err(WipeDeviceError::UnexpectedEvent { .. })
        ));
    }

    #[test]
    fn erases_root_contents() {
        // The root survives so a mounted volume stays mounted.
        let root = tempdir().unwrap();
        fs::create_dir_all(root.path().join("blobstore/objects")).unwrap();
        fs::write(root.path().join("blobstore/objects/one"), b"data").unwrap();
        fs::write(root.path().join("identity"), b"key").unwrap();
        assert!(purge(&[root.path().to_path_buf()]).is_empty());
        assert!(root.path().is_dir());
        assert_eq!(fs::read_dir(root.path()).unwrap().count(), 0);
    }

    #[test]
    fn ignores_missing_root() {
        let root = tempdir().unwrap();
        let missing = root.path().join("gone");
        assert!(purge(&[missing]).is_empty());
    }

    #[test]
    fn arms_once() {
        let wipe = DeviceWipe::new(vec![std::path::PathBuf::from("/tmp/aruna-wipe-test")]);
        assert!(!wipe.is_armed());
        wipe.arm();
        assert!(wipe.is_armed());
        assert_eq!(wipe.roots().len(), 1);
    }
}
