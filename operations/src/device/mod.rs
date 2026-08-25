//! Device-local plane of a user node: the offline authoring intake and the
//! owner's node controls. Nothing here is realm authority; a queued intent
//! becomes realm state only when the drain forwards it as an ordinary create.

pub mod compute;
pub mod delete_draft;
pub mod drain;
pub mod enqueue_draft;
pub mod inspect_draft;
pub mod list_drafts;
pub mod repository;
pub mod sync;
pub mod wipe;
