use anyhow::anyhow;
use anyhow::Result;
use async_channel::{Receiver, Sender, TryRecvError};
use digest::DynDigest;
use pithos_lib::helpers::notifications::{Message, Notifier};
use pithos_lib::transformer::{Transformer, TransformerType};
use std::collections::VecDeque;
use std::sync::Arc;
use tracing::error;

pub struct CrcTransformer<T: DynDigest + Send> {
    idx: Option<usize>,
    hasher: T,
    counter: u64,
    file_queue: Option<VecDeque<(usize, u64)>>,
    msg_receiver: Option<Receiver<Message>>,
    notifier: Option<Arc<Notifier>>,
    back_channel: Option<Sender<String>>,
    finished: bool,
}

impl<T> CrcTransformer<T>
where
    T: DynDigest + Send + Sync,
{
    #[tracing::instrument(level = "trace", skip(hasher))]
    #[allow(dead_code)]
    pub fn new(hasher: T, file_specific: bool) -> CrcTransformer<T> {
        let (file_queue, counter) = if file_specific {
            (Some(VecDeque::new()), 0)
        } else {
            (None, u64::MAX)
        };

        CrcTransformer {
            idx: None,
            hasher,
            counter,
            file_queue,
            msg_receiver: None,
            notifier: None,
            back_channel: None,
            finished: false,
        }
    }

    #[tracing::instrument(level = "trace", skip(hasher))]
    #[allow(dead_code)]
    pub fn new_with_backchannel(
        hasher: T,
        multifile: bool,
    ) -> (CrcTransformer<T>, Receiver<String>) {
        let (sx, rx) = async_channel::bounded(10);
        let (file_queue, counter) = if multifile {
            (Some(VecDeque::new()), 0)
        } else {
            (None, u64::MAX)
        };

        (
            CrcTransformer {
                idx: None,
                hasher,
                counter,
                file_queue,
                msg_receiver: None,
                notifier: None,
                back_channel: Some(sx),
                finished: false,
            },
            rx,
        )
    }

    #[tracing::instrument(level = "trace", skip(self))]
    fn process_messages(&mut self) -> Result<(bool, bool)> {
        if let Some(rx) = &self.msg_receiver {
            loop {
                match rx.try_recv() {
                    Ok(Message::Finished) => {
                        return Ok((true, false));
                    }
                    Ok(Message::ShouldFlush) => return Ok((false, true)),
                    Ok(Message::FileContext(ctx)) => {
                        if !ctx.is_dir && ctx.symlink_target.is_none() {
                            if let Some(queue) = self.file_queue.as_mut() {
                                queue.push_back((ctx.idx, ctx.decompressed_size));
                                if self.counter == 0 {
                                    self.counter = ctx.decompressed_size;
                                }
                            }
                        }
                    }
                    Ok(_) => {}
                    Err(TryRecvError::Empty) => {
                        break;
                    }
                    Err(TryRecvError::Closed) => {
                        error!("Message receiver closed");
                        return Err(anyhow!("Message receiver closed"));
                    }
                }
            }
        }
        Ok((false, false))
    }

    fn finalize_digest(&mut self) -> Result<()> {
        // Finish and reset digest. Send final checksum into backchannel if available.
        let finished_hash = self.hasher.finalize_reset().to_vec();
        if let Some(sx) = &self.back_channel {
            sx.try_send(hex::encode(finished_hash))?;
        }
        Ok(())
    }

    fn fetch_next(&mut self, carryover_bytes: &[u8]) -> Result<()> {
        if let Some(file_queue) = self.file_queue.as_mut() {
            // Remove current file from queue
            file_queue.pop_front();
            // Try to fetch next file context if available and init digest with carryover bytes
            if let Some((_, size)) = file_queue.front() {
                self.counter = *size - carryover_bytes.len() as u64; // Already subtract the carryover bytes
                DynDigest::update(&mut self.hasher, carryover_bytes);
            } else {
                // No more files available -> finished
                self.finish()?
            }
        } else {
            // No file queue available -> finished
            self.finish()?
        }

        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        self.finished = true;
        if let Some(notifier) = self.notifier.clone() {
            notifier.send_next(
                self.idx.ok_or_else(|| anyhow!("Missing idx"))?,
                Message::Finished,
            )?;
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl<T> Transformer for CrcTransformer<T>
where
    T: DynDigest + Send + Sync,
{
    #[tracing::instrument(level = "trace", skip(self))]
    async fn initialize(&mut self, idx: usize) -> (TransformerType, Sender<Message>) {
        self.idx = Some(idx);
        let (sx, rx) = async_channel::bounded(10);
        self.msg_receiver = Some(rx);
        (TransformerType::Hashing, sx)
    }

    #[tracing::instrument(level = "trace", skip(self, buf))]
    async fn process_bytes(&mut self, buf: &mut bytes::BytesMut) -> Result<()> {
        let Ok((finished, should_flush)) = self.process_messages() else {
            return Err(anyhow!("[HashingTransformer] Error processing messages"));
        };

        // Do nothing if already finished
        if !self.finished {
            // If buffer contains more bytes than needed
            let carryover = if buf.len() as u64 >= self.counter {
                // Fetch remaining and carryover from buf
                let remaining = buf.get(..self.counter as usize).unwrap_or_default();
                let carryover = buf.get(self.counter as usize..).unwrap_or_default();

                // Update current digest with remaining and finalize
                DynDigest::update(&mut self.hasher, remaining);
                self.counter = 0;
                carryover
            } else {
                // Just digest bytes from buf
                self.counter -= buf.len() as u64;
                DynDigest::update(&mut self.hasher, &buf);
                &[]
            };

            if finished && !self.finished {
                // Finalize digest and finish transformer
                self.finalize_digest()?;
                self.finish()?;
            } else if self.counter == 0 || should_flush {
                // Init new digest with carryover bytes of next file
                self.finalize_digest()?;
                self.fetch_next(carryover)?
            }
        }

        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self, notifier))]
    #[inline]
    async fn set_notifier(&mut self, notifier: Arc<Notifier>) -> Result<()> {
        self.notifier = Some(notifier);
        Ok(())
    }
}
