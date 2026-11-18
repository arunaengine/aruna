//! Answers the plugin requests other nodes send over the metadata transport.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::NodeId;
use aruna_core::plugins::{NodeCheckError, ReleaseError};
use aruna_core::time::unix_timestamp_millis;

use super::serving_node;
use crate::driver::{DriverContext, drive};
use crate::metadata::protocol::{MetadataReadError, MetadataTransportMessage};
use crate::plugins::artifacts::{ServeFetch, ServeStore};
use crate::plugins::bound::ServeNodeCheck;
use crate::plugins::clock::ServeClock;
use crate::plugins::decide::ServeEvents;
use crate::plugins::secret_use::ServeRelease;

/// The answer to a plugin request, or the message back when it is none.
pub(crate) async fn serve_peer(
    context: &Arc<DriverContext>,
    peer: NodeId,
    message: MetadataTransportMessage,
) -> Result<MetadataTransportMessage, MetadataTransportMessage> {
    match message {
        MetadataTransportMessage::StoreArtifact { chunk } => {
            let stored = async {
                let node = serving_node(context).ok_or(MetadataReadError::Unavailable)?;
                let now_ms = unix_timestamp_millis();
                drive(ServeStore::new(node, peer, chunk, now_ms), context).await
            };
            let result = Box::pin(stored).await;
            Ok(MetadataTransportMessage::ArtifactStored { result })
        }
        MetadataTransportMessage::FetchArtifact { digest, index } => {
            let fetched = async {
                let node = serving_node(context).ok_or(MetadataReadError::Unavailable)?;
                drive(ServeFetch::new(node, peer, digest, index), context).await
            };
            let result = Box::pin(fetched).await;
            Ok(MetadataTransportMessage::FetchedArtifact { result })
        }
        MetadataTransportMessage::ProbeClock => {
            let clock = async {
                let node = serving_node(context).ok_or(MetadataReadError::Unavailable)?;
                drive(
                    ServeClock::new(node, peer, unix_timestamp_millis()),
                    context,
                )
                .await
            };
            let result = Box::pin(clock).await;
            Ok(MetadataTransportMessage::ClockProbed { result })
        }
        MetadataTransportMessage::ForwardEvents { batch } => {
            let decided = async {
                let node = serving_node(context).ok_or(MetadataReadError::Unavailable)?;
                let now_ms = unix_timestamp_millis();
                drive(ServeEvents::new(node, peer, batch, now_ms), context).await
            };
            let result = Box::pin(decided).await;
            Ok(MetadataTransportMessage::EventsDecided { result })
        }
        MetadataTransportMessage::ReleaseSecret { request } => {
            let released = async {
                let unavailable = || ReleaseError::Unavailable("no network".to_string());
                let node = serving_node(context).ok_or_else(unavailable)?;
                let serve = ServeRelease::new(node, peer, request);
                drive(serve, context).await.map_err(|failure| failure.0)
            };
            let result = Box::pin(released).await;
            Ok(MetadataTransportMessage::SecretReleased { result })
        }
        MetadataTransportMessage::CheckNodeSecret { name, kind } => {
            let checked = async {
                let node = serving_node(context).ok_or(NodeCheckError::Unavailable)?;
                let serve = ServeNodeCheck::new(node, peer, name, kind);
                drive(serve, context).await.map_err(|failure| failure.0)
            };
            let result = Box::pin(checked).await;
            Ok(MetadataTransportMessage::NodeSecretChecked { result })
        }
        other => Err(other),
    }
}
