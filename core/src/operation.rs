use crate::{events::Event, types::Effects};

pub trait Operation: Send {
    type Output: Send;
    type Error: Send;

    fn start(&mut self) -> Effects;
    fn step(&mut self, events: Event) -> Effects;
    fn is_complete(&self) -> bool;
    fn finalize(self) -> Result<Self::Output, Self::Error>;
    fn abort(&mut self) -> Effects;
}
