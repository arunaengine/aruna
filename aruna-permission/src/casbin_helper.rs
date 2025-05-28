use tracing::warn;

use crate::error::ArunaPermissionHandlerError;

pub struct CasbinPolicy(Option<Vec<String>>);

impl CasbinPolicy {
    pub fn new(policy: Vec<String>) -> Result<Self, ArunaPermissionHandlerError> {
        if policy.is_empty() {
            return Err(ArunaPermissionHandlerError::CasbinHelperError(
                "Failed to create CasbinPolicy, empty policies".to_string(),
            ));
        }
        Ok(CasbinPolicy(Some(policy)))
    }

    pub fn commit(mut self) -> Vec<String> {
        // This will consume the CasbinPolicy and return the policies
        // It should be impossible to call this method if the policy is empty
        self.0.take().unwrap_or_default()
    }

    pub fn abort(self) {
        let _ = self.commit();
    }
}

impl Drop for CasbinPolicy {
    fn drop(&mut self) {
        if let Some(policy) = self.0.take() {
            warn!(
                "Dropping CasbinPolicy without committing / aborting: {:?}",
                policy
            );
        }
    }
}

pub struct CasbinRole(Option<Vec<String>>);

impl CasbinRole {
    pub fn new(policy: Vec<String>) -> Result<Self, ArunaPermissionHandlerError> {
        if policy.is_empty() {
            return Err(ArunaPermissionHandlerError::CasbinHelperError(
                "Failed to create CasbinRole, empty policies".to_string(),
            ));
        }
        Ok(CasbinRole(Some(policy)))
    }

    pub fn commit(mut self) -> Vec<String> {
        self.0.take().unwrap_or_default()
    }

    pub fn abort(self) {
        let _ = self.commit();
    }
}

impl Drop for CasbinRole {
    fn drop(&mut self) {
        if let Some(policy) = self.0.take() {
            warn!(
                "Dropping CasbinRole without committing / aborting: {:?}",
                policy
            );
        }
    }
}
