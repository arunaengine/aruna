use std::collections::VecDeque;

use tokio::sync::mpsc;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;

use super::constants::{DRIVER_STEP_BUDGET, DRIVER_TICK_INTERVAL};
use super::protocol::{DhtCmd, DhtEffect, DhtInput, DhtIo, DhtIoError, DhtIoRequest, DhtOutput};
use super::state::DhtStateMachine;

#[derive(Debug)]
pub struct DhtDriver {
    state: DhtStateMachine,
    cmd_rx: mpsc::Receiver<DhtCmd>,
    io_rx: mpsc::Receiver<DhtIo>,
    io_request_tx: mpsc::Sender<DhtIoRequest>,
    output_tx: mpsc::UnboundedSender<DhtOutput>,
    shutdown: CancellationToken,
    now_tick: u64,
    pending_inputs: VecDeque<DhtInput>,
}

impl DhtDriver {
    pub fn new(
        state: DhtStateMachine,
        cmd_rx: mpsc::Receiver<DhtCmd>,
        io_rx: mpsc::Receiver<DhtIo>,
        io_request_tx: mpsc::Sender<DhtIoRequest>,
        output_tx: mpsc::UnboundedSender<DhtOutput>,
        shutdown: CancellationToken,
    ) -> Self {
        Self {
            state,
            cmd_rx,
            io_rx,
            io_request_tx,
            output_tx,
            shutdown,
            now_tick: 0,
            pending_inputs: VecDeque::new(),
        }
    }

    pub async fn run(mut self) {
        let mut ticker = tokio::time::interval(DRIVER_TICK_INTERVAL);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let _ = ticker.tick().await;

        loop {
            if self.drain_pending_inputs() {
                tokio::task::yield_now().await;
                continue;
            }

            tokio::select! {
                biased;
                _ = self.shutdown.cancelled() => {
                    self.pending_inputs.push_back(DhtInput::ShutdownRequested);
                    break;
                }
                maybe_cmd = self.cmd_rx.recv() => {
                    let Some(cmd) = maybe_cmd else {
                        self.pending_inputs.push_back(DhtInput::ShutdownRequested);
                        break;
                    };
                    self.pending_inputs.push_back(DhtInput::Cmd(cmd));
                }
                maybe_io = self.io_rx.recv() => {
                    let Some(io) = maybe_io else {
                        self.pending_inputs.push_back(DhtInput::Io(DhtIo::DispatcherClosed));
                        continue;
                    };
                    self.pending_inputs.push_back(DhtInput::Io(io));
                }
                _ = ticker.tick() => {
                    self.now_tick = self.now_tick.saturating_add(1);
                    self.pending_inputs
                        .push_back(DhtInput::Tick { now_tick: self.now_tick });
                }
            }
        }

        while self.drain_pending_inputs() {
            tokio::task::yield_now().await;
        }
    }

    fn drain_pending_inputs(&mut self) -> bool {
        let mut processed_steps = 0usize;
        while let Some(next_input) = self.pending_inputs.pop_front() {
            if processed_steps >= DRIVER_STEP_BUDGET {
                break;
            }
            processed_steps = processed_steps.saturating_add(1);

            let effects = self.state.step(next_input);
            for effect in effects {
                match effect {
                    DhtEffect::IoRequest(request) => {
                        if let Some(recovery_input) = self.try_send_io_request(request) {
                            self.pending_inputs.push_back(recovery_input);
                        }
                    }
                    DhtEffect::Output(output) => {
                        let _ = self.output_tx.send(output);
                    }
                }
            }
        }

        !self.pending_inputs.is_empty()
    }

    fn try_send_io_request(&self, request: DhtIoRequest) -> Option<DhtInput> {
        match self.io_request_tx.try_send(request.clone()) {
            Ok(()) => None,
            Err(mpsc::error::TrySendError::Full(failed_request)) => {
                self.synthetic_io_error(failed_request, DhtIoError::QueueFull)
            }
            Err(mpsc::error::TrySendError::Closed(failed_request)) => {
                self.synthetic_io_error(failed_request, DhtIoError::DispatcherClosed)
            }
        }
    }

    fn synthetic_io_error(&self, request: DhtIoRequest, error: DhtIoError) -> Option<DhtInput> {
        match request {
            DhtIoRequest::RpcRequest {
                request_id, peer, ..
            } => Some(DhtInput::Io(DhtIo::RpcError {
                request_id,
                peer,
                error,
            })),
            DhtIoRequest::RpcResponse { inbound_id, .. }
            | DhtIoRequest::DropInbound { inbound_id } => {
                Some(DhtInput::Io(DhtIo::InboundDropped { inbound_id }))
            }
            DhtIoRequest::StorageRead { storage_id, .. }
            | DhtIoRequest::StorageWrite { storage_id, .. }
            | DhtIoRequest::StorageDelete { storage_id, .. }
            | DhtIoRequest::StorageIter { storage_id, .. } => {
                Some(DhtInput::Io(DhtIo::StorageError { storage_id, error }))
            }
        }
    }
}
