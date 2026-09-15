//! Metadata handle adapter: a node without a metadata plane keeps the explicit
//! `HandleMissing` outcome instead of dropping the effect.

use aruna_core::effects::Effect;
use aruna_core::events::Event;
use aruna_core::handle::Handle;
use aruna_core::metadata::{MetadataEffect, MetadataError, MetadataEvent};

use crate::driver::DriverContext;

pub(super) async fn dispatch_metadata(effect: MetadataEffect, context: &DriverContext) -> Event {
    if let Some(metadata_handle) = &context.metadata_handle {
        Box::pin(metadata_handle.send_effect(Effect::Metadata(effect))).await
    } else {
        Event::Metadata(MetadataEvent::Error {
            graph_iri: None,
            error: MetadataError::HandleMissing,
        })
    }
}
