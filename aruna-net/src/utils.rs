use crate::actor::CHANNEL_SIZE;

pub struct ChannelPair<T> {
    pub sender: async_channel::Sender<T>,
    pub receiver: async_channel::Receiver<T>,
}

impl<T> ChannelPair<T> {
    #[tracing::instrument(level = "trace")]
    pub fn new() -> Self {
        let (sender, receiver) = async_channel::bounded(CHANNEL_SIZE);
        Self { sender, receiver }
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn receiver(&self) -> &async_channel::Receiver<T> {
        &self.receiver
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn sender(&self) -> &async_channel::Sender<T> {
        &self.sender
    }
}

impl<T> Clone for ChannelPair<T> {
    #[tracing::instrument(level = "trace", skip(self))]
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            receiver: self.receiver.clone(),
        }
    }
}
