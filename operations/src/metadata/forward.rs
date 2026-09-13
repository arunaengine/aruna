mod admin;
mod authorize;
mod bucket;
mod device;
mod pid;
mod read;
mod replay;
mod routing;
mod transport;
mod write;

pub use self::admin::{
    ForwardGroupError, forward_group_create, forward_token_revoke, relay_admin_event,
};
pub(crate) use self::admin::{apply_admin_relay, apply_group_create, apply_token_revoke};
pub(crate) use self::authorize::{
    authorize_forwarded_caller, forward_auth_error, is_sync_eligible, peer_acts_for,
};
pub(crate) use self::bucket::apply_bucket_create;
pub(crate) use self::device::{apply_device_batch, serve_graph_state, serve_realm_documents};
pub(crate) use self::pid::apply_forwarded_pid;
pub use self::pid::{
    fail_pid_routed, mint_pid_routed, read_pid_routed, resolve_pid_routed, submit_pid_routed,
    withdraw_pid_routed,
};
pub(crate) use self::read::{
    AuthFailure, ReadDecision, apply_document_query, apply_forwarded_export,
    apply_forwarded_profile, export_profile_routed, reduce_holder_reads,
};
pub use self::read::{
    admits_profile_peer, export_profile_local, export_rocrate_routed, get_metadata_routed,
    route_profile_status,
};
pub use self::routing::{MetadataWriteRoute, is_user_origin, origin_holds_document, write_route};
pub use self::transport::MetadataWriteError;
pub(crate) use self::transport::{forward_to_holders, read_error};
pub(crate) use self::write::apply_forwarded_write;
pub use self::write::{
    CreateMetadataAuthorizedError, apply_batch_routed, create_metadata_authorized,
    route_metadata_create, route_metadata_delete, route_metadata_update,
};

#[cfg(test)]
mod tests;
