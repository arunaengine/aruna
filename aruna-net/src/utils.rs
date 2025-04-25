use crate::connection_handler::CHANNEL_SIZE;

pub struct ChannelPair<T> {
    pub sender: async_channel::Sender<T>,
    pub receiver: async_channel::Receiver<T>,
}

impl<T> ChannelPair<T> {
    pub fn new() -> Self {
        let (sender, receiver) = async_channel::bounded(CHANNEL_SIZE);
        Self { sender, receiver }
    }

    pub fn receiver(&self) -> &async_channel::Receiver<T> {
        &self.receiver
    }
    
    pub fn sender(&self) -> &async_channel::Sender<T> {
        &self.sender
    }
}