#[path = "forward_read.rs"]
mod read;
#[path = "forward_write.rs"]
mod write;

pub(crate) use self::read::{
    AuthFailure, ReadDecision, apply_document_query, apply_forwarded_export,
    apply_forwarded_profile, export_profile_routed, reduce_holder_reads,
};
pub use self::read::{
    admits_profile_peer, export_profile_local, export_rocrate_routed, get_metadata_routed,
    route_profile_status,
};
pub(crate) use self::write::apply_forwarded_write;
pub use self::write::{
    CreateAuthorizedError, apply_batch_routed, create_metadata_authorized, route_metadata_create,
    route_metadata_delete, route_metadata_update,
};

#[cfg(test)]
#[path = "forward_tests.rs"]
mod tests;
