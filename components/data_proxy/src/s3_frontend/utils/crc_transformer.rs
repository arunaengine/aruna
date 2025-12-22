use anyhow::anyhow;
use anyhow::Result;
use base64::prelude::*;
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
            sx.try_send(BASE64_STANDARD.encode(&finished_hash))?;
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
                DynDigest::update(&mut self.hasher, buf);
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

#[cfg(test)]
mod test {
    use crate::s3_frontend::utils::crc_transformer::CrcTransformer;
    use crc_fast::{CrcAlgorithm, Digest as CrcDigest};
    use digest::Digest;
    use pithos_lib::helpers::notifications::Message;
    use pithos_lib::helpers::structs::{EncryptionKey, FileContext};
    use pithos_lib::readwrite::GenericReadWriter;
    use pithos_lib::transformer::ReadWriter;
    use pithos_lib::transformers::footer::FooterGenerator;
    use pithos_lib::transformers::hashing_transformer::HashingTransformer;
    use pithos_lib::transformers::pithos_comp_enc::PithosTransformer;
    use pithos_lib::transformers::size_probe::SizeProbe;
    use sha2::Sha256;

    #[tokio::test]
    async fn test_transformer_crc32() {
        let input = b"This is some super important data".to_vec();
        let mut output = Vec::new();

        let mut asr = GenericReadWriter::new_with_writer(input.as_ref(), &mut output);
        let (crc32_transformer, crc32_recv) =
            CrcTransformer::new_with_backchannel(CrcDigest::new(CrcAlgorithm::Crc32IsoHdlc), false);

        asr = asr.add_transformer(crc32_transformer);
        asr.process().await.unwrap();

        let crc32 = crc32_recv.try_recv().unwrap();
        assert_eq!(crc32, "6a21ac65")
    }

    #[tokio::test]
    async fn test_transformer_crc32c() {
        let input = b"This is some super important data".to_vec();
        let mut output = Vec::new();

        let mut asr = GenericReadWriter::new_with_writer(input.as_ref(), &mut output);
        let (crc32c_transformer, crc32c_recv) =
            CrcTransformer::new_with_backchannel(CrcDigest::new(CrcAlgorithm::Crc32Iscsi), false);

        asr = asr.add_transformer(crc32c_transformer);
        asr.process().await.unwrap();

        let crc32c = crc32c_recv.try_recv().unwrap();

        assert_eq!(crc32c, "271fd1c3")
    }

    #[tokio::test]
    async fn test_transformer_crc64nvme() {
        let input = b"This is some super important data".to_vec();
        let mut output = Vec::new();

        let mut asr = GenericReadWriter::new_with_writer(input.as_ref(), &mut output);
        let (crc64nvme_transformer, crc64nvme_recv) =
            CrcTransformer::new_with_backchannel(CrcDigest::new(CrcAlgorithm::Crc64Nvme), false);

        asr = asr.add_transformer(crc64nvme_transformer);
        asr.process().await.unwrap();

        let crc64nvme = crc64nvme_recv.try_recv().unwrap();

        assert_eq!(crc64nvme, "9c6658ea321baf04")
    }

    #[tokio::test]
    async fn test_transformer_larger_input() {
        let input = b"abcde".repeat(1234567).to_vec();
        let mut output = Vec::new();

        let mut asr = GenericReadWriter::new_with_writer(input.as_ref(), &mut output);
        let (crc32_transformer, crc32_recv) =
            CrcTransformer::new_with_backchannel(CrcDigest::new(CrcAlgorithm::Crc32IsoHdlc), false);
        let (crc32c_transformer, crc32c_recv) =
            CrcTransformer::new_with_backchannel(CrcDigest::new(CrcAlgorithm::Crc32Iscsi), false);
        let (crc64nvme_transformer, crc64nvme_recv) =
            CrcTransformer::new_with_backchannel(CrcDigest::new(CrcAlgorithm::Crc64Nvme), false);
        asr = asr.add_transformer(crc32_transformer);
        asr = asr.add_transformer(crc32c_transformer);
        asr = asr.add_transformer(crc64nvme_transformer);
        asr.process().await.unwrap();

        let crc32 = crc32_recv.try_recv().unwrap();
        let crc32c = crc32c_recv.try_recv().unwrap();
        let crc64nvme = crc64nvme_recv.try_recv().unwrap();

        assert_eq!(crc32, "3746087e");
        assert_eq!(crc32c, "871b47d3");
        assert_eq!(crc64nvme, "b6275d2fb6d88d11")
    }

    #[tokio::test]
    async fn test_transformer_multifile() {
        let file1 = b"Lorem ipsum dolor sit amet, consetetur sadipscing elitr.".to_vec();
        let file2 = b"Stet clita kasd gubergren, no sea takimata sanctus.".to_vec();
        let combined = Vec::from_iter(file1.clone().into_iter().chain(file2.clone()));

        let (sx, rx) = async_channel::bounded(10);
        sx.send(Message::FileContext(FileContext {
            file_path: "file1.txt".to_string(),
            compressed_size: file1.len() as u64,
            decompressed_size: file1.len() as u64,
            compression: true,
            ..Default::default()
        }))
        .await
        .unwrap();

        sx.send(Message::FileContext(FileContext {
            file_path: "file2.txt".to_string(),
            compressed_size: file2.len() as u64,
            decompressed_size: file2.len() as u64,
            compression: false,
            ..Default::default()
        }))
        .await
        .unwrap();

        // Create a new GenericReadWriter
        let mut output: Vec<u8> = Vec::new();
        let mut asr = GenericReadWriter::new_with_writer(combined.as_ref(), &mut output);
        asr.add_message_receiver(rx).await.unwrap();

        let (crc32_transformer, crc32_recv) =
            CrcTransformer::new_with_backchannel(CrcDigest::new(CrcAlgorithm::Crc32IsoHdlc), true);
        asr = asr.add_transformer(crc32_transformer);
        asr.process().await.unwrap();

        let crc32_file1 = crc32_recv.try_recv().unwrap();
        let crc32_file2 = crc32_recv.try_recv().unwrap();

        assert_eq!(crc32_file1, "11382a6b");
        assert_eq!(crc32_file2, "bb283921");
    }

    #[tokio::test]
    async fn test_transformer_multifile_larger() {
        let file1 = b"abcdefg".repeat(1024 * 1024).to_vec();
        let file2 = b"zyxwvu".repeat(1024 * 1024).to_vec();
        let combined = Vec::from_iter(file1.clone().into_iter().chain(file2.clone()));

        let (sx, rx) = async_channel::bounded(10);
        sx.send(Message::FileContext(FileContext {
            file_path: "file1.txt".to_string(),
            compressed_size: file1.len() as u64,
            decompressed_size: file1.len() as u64,
            compression: true,
            ..Default::default()
        }))
        .await
        .unwrap();

        sx.send(Message::FileContext(FileContext {
            file_path: "file2.txt".to_string(),
            compressed_size: file2.len() as u64,
            decompressed_size: file2.len() as u64,
            compression: false,
            ..Default::default()
        }))
        .await
        .unwrap();

        // Create a new GenericReadWriter
        let mut output: Vec<u8> = Vec::new();
        let mut asr = GenericReadWriter::new_with_writer(combined.as_ref(), &mut output);
        asr.add_message_receiver(rx).await.unwrap();

        let (crc32_transformer, crc32_recv) =
            CrcTransformer::new_with_backchannel(CrcDigest::new(CrcAlgorithm::Crc32IsoHdlc), true);
        asr = asr.add_transformer(crc32_transformer);
        asr.process().await.unwrap();

        let crc32_file1 = crc32_recv.try_recv().unwrap();
        let crc32_file2 = crc32_recv.try_recv().unwrap();

        assert_eq!(crc32_file1, "48376d21");
        assert_eq!(crc32_file2, "9de97047");
    }

    #[tokio::test]
    async fn test_transformer_proxy_mock() {
        let input = b"This is some super important data".to_vec();
        let mut output = Vec::new();

        let mut asr = GenericReadWriter::new_with_writer(input.as_ref(), &mut output);
        let (uncompressed_probe, _) = SizeProbe::new();
        let (sha_transformer, _) =
            HashingTransformer::new_with_backchannel(Sha256::new(), "sha256".to_string());
        let (crc64nvme_transformer, crc64nvme_recv) =
            CrcTransformer::new_with_backchannel(CrcDigest::new(CrcAlgorithm::Crc64Nvme), false);

        let (tx, rx) = async_channel::bounded(10);
        asr.add_message_receiver(rx).await.unwrap();

        tx.send(Message::FileContext(FileContext {
            idx: 0,
            file_path: "file1.txt".to_string(),
            compressed_size: input.len() as u64,
            decompressed_size: input.len() as u64,
            compression: false,
            recipients_pubkeys: vec![],
            encryption_key: EncryptionKey::Same(
                b"wvwj3485nxgyq5ub9zd3e7jsrq7a92ea"
                    .to_vec()
                    .try_into()
                    .unwrap(),
            ),
            ..Default::default()
        }))
        .await
        .unwrap();

        asr = asr.add_transformer(uncompressed_probe);
        asr = asr.add_transformer(sha_transformer);
        asr = asr.add_transformer(crc64nvme_transformer);
        asr = asr.add_transformer(PithosTransformer::new());
        asr = asr.add_transformer(FooterGenerator::new(None));
        asr.process().await.unwrap();

        let crc64nvme = crc64nvme_recv.try_recv().unwrap();
        assert_eq!(crc64nvme, "9c6658ea321baf04")
    }
}
