//! Node startup phases and the resources they own.
//!
//! The ordered flow lives in [`crate::application`]. Modules here own the
//! acquired resources, realm preparation, listener binding, background work,
//! and the startup instrumentation that retained binary tests need.

pub mod background;
pub mod listeners;
pub mod realm;
pub mod resources;
pub mod test_hooks;

pub use resources::NodeResources;
