pub mod error;
pub mod manager;
pub mod paths;
pub mod token;

mod casbin;
mod casbin_helper;

// Re-export commonly used types
pub use casbin::DBNAME;
pub use error::{PermissionError, Result, UnificationError};
pub use manager::{
    Action, IDENTITY_PERMISSIONS_DB, OIDC_IDENTITIES_DB, PermissionManager, RESOURCE_DB,
    ResourceId, UserIdentity,
};
pub use paths::Path;

// Token system exports - simple and focused
pub use token::{OidcToken, RealmTokenClaims, TokenSystem};
