use serde::{Deserialize, Serialize};

/// Identifier for a state machine runtime.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum StateMachineId {
    IncomingGossipMessage,
    Named(String),
}

/// Generic bootstrap configuration for state-machine execution.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StateMachineConfig {
    pub id: StateMachineId,
    pub bootstrap: Option<Vec<u8>>,
}

impl StateMachineConfig {
    #[must_use]
    pub fn new(id: StateMachineId) -> Self {
        Self {
            id,
            bootstrap: None,
        }
    }

    #[must_use]
    pub fn incoming_gossip_message() -> Self {
        Self::new(StateMachineId::IncomingGossipMessage)
    }

    #[must_use]
    pub fn named(name: impl Into<String>) -> Self {
        Self::new(StateMachineId::Named(name.into()))
    }

    #[must_use]
    pub fn with_bootstrap(mut self, bootstrap: Vec<u8>) -> Self {
        self.bootstrap = Some(bootstrap);
        self
    }
}

impl Default for StateMachineConfig {
    fn default() -> Self {
        Self::incoming_gossip_message()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_named_state_machine_config() {
        let config = StateMachineConfig::named("permission");
        assert!(matches!(config.id, StateMachineId::Named(_)));
        assert!(config.bootstrap.is_none());
    }

    #[test]
    fn test_bootstrap_payload() {
        let config = StateMachineConfig::incoming_gossip_message().with_bootstrap(vec![1, 2, 3]);
        assert_eq!(config.bootstrap, Some(vec![1, 2, 3]));
    }
}
