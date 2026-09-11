//! Device-local plane of a user node: the offline authoring intake and the
//! owner's node controls. Nothing here is realm authority; a queued intent
//! becomes realm state only when the drain forwards it as an ordinary create.

pub mod backlog;
pub mod compute;
pub mod delete_draft;
pub mod drain;
pub mod edit;
pub mod enqueue_draft;
pub mod inspect_draft;
pub mod intake;
pub mod list_drafts;
pub mod realm_documents;
pub mod refresh;
pub mod remove_device_node;
pub mod replica;
pub mod selection;
pub mod sync;
pub mod sync_status;
#[cfg(test)]
mod tests;
pub mod wipe;
