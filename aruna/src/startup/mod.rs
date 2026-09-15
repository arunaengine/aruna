//! Node startup phases and the resources they own. The ordered flow lives in
//! [`crate::application`]; modules here own acquired resources, realm
//! preparation, listener binding, background work, and test instrumentation.

pub mod background;
pub mod listeners;
pub mod realm;
pub mod resources;
pub mod test_hooks;

pub use resources::NodeResources;
