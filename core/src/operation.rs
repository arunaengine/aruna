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

pub trait SubOperation: Send {
    fn start(&mut self) -> Effects;
    fn step(&mut self, event: Event) -> Effects;
    fn is_complete(&self) -> bool;
    fn finalize(self: Box<Self>) -> Event;
    fn abort(&mut self) -> Effects;
}

struct MappedSubOperation<O, F>
where
    O: Operation,
    F: FnOnce(Result<O::Output, O::Error>) -> Event + Send,
{
    operation: Option<O>,
    map_result: Option<F>,
}

impl<O, F> MappedSubOperation<O, F>
where
    O: Operation,
    F: FnOnce(Result<O::Output, O::Error>) -> Event + Send,
{
    fn new(operation: O, map_result: F) -> Self {
        Self {
            operation: Some(operation),
            map_result: Some(map_result),
        }
    }
}

impl<O, F> SubOperation for MappedSubOperation<O, F>
where
    O: Operation + 'static,
    O::Output: Send + 'static,
    O::Error: Send + 'static,
    F: FnOnce(Result<O::Output, O::Error>) -> Event + Send + 'static,
{
    fn start(&mut self) -> Effects {
        self.operation
            .as_mut()
            .expect("suboperation must be present in start")
            .start()
    }

    fn step(&mut self, event: Event) -> Effects {
        self.operation
            .as_mut()
            .expect("suboperation must be present in step")
            .step(event)
    }

    fn is_complete(&self) -> bool {
        self.operation
            .as_ref()
            .expect("suboperation must be present in is_complete")
            .is_complete()
    }

    fn finalize(self: Box<Self>) -> Event {
        let mut this = *self;
        let operation = this
            .operation
            .take()
            .expect("suboperation must be present in finalize");
        let map_result = this
            .map_result
            .take()
            .expect("result mapper must be present in finalize");
        map_result(operation.finalize())
    }

    fn abort(&mut self) -> Effects {
        self.operation
            .as_mut()
            .expect("suboperation must be present in abort")
            .abort()
    }
}

pub fn boxed_suboperation<O, F>(operation: O, map_result: F) -> Box<dyn SubOperation>
where
    O: Operation + 'static,
    O::Output: Send + 'static,
    O::Error: Send + 'static,
    F: FnOnce(Result<O::Output, O::Error>) -> Event + Send + 'static,
{
    Box::new(MappedSubOperation::new(operation, map_result))
}
