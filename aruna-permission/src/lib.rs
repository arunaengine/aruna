pub mod error;
pub mod manager;
pub mod paths;

mod casbin;
mod casbin_helper;

// Re-export commonly used types for integration tests
pub use casbin::DBNAME;
pub use error::{PermissionError, Result};
pub use manager::{
    Action, IDENTITY_PERMISSIONS_DB, OIDC_IDENTITIES_DB, PermissionManager, RESOURCE_DB,
    ResourceId, UserIdentity,
};
pub use paths::Path;
