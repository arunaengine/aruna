use aruna_permission::UserIdentity;
use aruna_storage::storage::store::Store;
use iroh::endpoint::{RecvStream, SendStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::network_trait::{MetadataMessage, Network};
use crate::{
    error::ArunaMetadataError,
    models::requests::ForwardRequest,
    persistence::search::search::Search,
    transactions::{controller::Controller, request::Request},
};

pub(super) async fn read_message(
    recv_stream: &mut RecvStream,
) -> Result<MetadataMessage, ArunaMetadataError> {
    let len = recv_stream.read_u32().await?;
    let mut buf = vec![0; len as usize];

    recv_stream
        .read_exact(&mut buf)
        .await
        .map_err(|e| ArunaMetadataError::NetworkError(e.to_string()))?;
    let message = postcard::from_bytes::<MetadataMessage>(&buf).map_err(|e| {
        ArunaMetadataError::NetworkError(format!("Failed to deserialize message: {e:#}"))
    })?;
    Ok(message)
}

pub(super) async fn send_message(
    message: MetadataMessage,
    send_stream: &mut SendStream,
) -> Result<(), ArunaMetadataError> {
    let response = postcard::to_allocvec(&message)?;
    send_stream.write_u32(response.len() as u32).await?;
    send_stream
        .write_all(&response)
        .await
        .map_err(|e| ArunaMetadataError::NetworkError(e.to_string()))?;
    Ok(())
}

impl ForwardRequest {
    pub async fn authorize<St, Se, N>(
        &self,
        token: Option<String>,
        controller: &Controller<St, Se, N>,
    ) -> Result<Option<UserIdentity>, ArunaMetadataError>
    where
        for<'a> St: Store<'a> + 'static,
        Se: Search + 'static,
        N: Network + 'static,
    {
        let auth_context = match self {
            ForwardRequest::GetResource(r) => r.authorize(token, controller).await?,
            ForwardRequest::UpdateResource(r) => r.authorize(token, controller).await?,
            ForwardRequest::Search(r) => r.authorize(token, controller).await?,
        };

        Ok(auth_context)
    }
}
