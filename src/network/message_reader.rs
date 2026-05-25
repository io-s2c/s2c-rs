use crate::network::error::NetworkError;
use crate::proto::io::S2cMessage;
use prost::Message;
use std::collections::VecDeque;
use tokio::io::{AsyncReadExt, BufReader};
use tokio::net::tcp::OwnedReadHalf;

pub trait S2cMessageReader {
    fn read_next_message(
        &mut self,
        reader: &mut BufReader<OwnedReadHalf>,
    ) -> impl Future<Output = Result<S2cMessage, NetworkError>> + Send;
}

pub struct S2CMessageReaderImpl {
    max_message_size: u32,
    buf: Vec<u8>,
}

impl S2CMessageReaderImpl {
    pub fn new(max_message_size: u32) -> Self {
        Self {
            max_message_size,
            buf: vec![0u8; 1024],
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq)]
pub enum Failure {
    Io,
    Drop,
}

pub struct S2CMessageReaderTestWrapper<F, I>
where
    F: FnMut() -> usize,
    I: Fn(&S2cMessage),
{
    inner: S2CMessageReaderImpl,
    failures: VecDeque<Failure>,
    reading_counter: usize,
    fail_every: F,
    message_inspector: I,
}

impl<F, I> S2CMessageReaderTestWrapper<F, I>
where
    F: FnMut() -> usize,
    I: Fn(&S2cMessage),
{
    pub fn new(
        inner: S2CMessageReaderImpl,
        failures: VecDeque<Failure>,
        fail_every: F,
        message_inspector: I,
    ) -> Self {
        Self {
            inner,
            failures,
            reading_counter: 0,
            fail_every,
            message_inspector,
        }
    }
}

impl S2cMessageReader for S2CMessageReaderImpl {
    async fn read_next_message(
        &mut self,
        reader: &mut BufReader<OwnedReadHalf>,
    ) -> Result<S2cMessage, NetworkError> {
        let msg_size = reader
            .read_u32()
            .await
            .map_err(|e| NetworkError::Io { err: e.to_string() })?;
        if msg_size > self.max_message_size {
            let mut skip_stream = reader.take(msg_size as u64);
            tokio::io::copy(&mut skip_stream, &mut tokio::io::sink())
                .await
                .map_err(|e| NetworkError::Io { err: e.to_string() })?;
            return Err(NetworkError::MessageTooLarge {
                size: msg_size,
                max: self.max_message_size,
            });
        }

        if self.buf.len() < msg_size as usize {
            self.buf.resize(msg_size as usize, 0u8);
        }
        reader
            .read_exact(&mut self.buf[..msg_size as usize])
            .await
            .map_err(|e| NetworkError::Io { err: e.to_string() })?;
        S2cMessage::decode(&self.buf[..msg_size as usize])
            .map_err(|e| NetworkError::InvalidProtobuf { err: e.to_string() })
    }
}

impl<F, I> S2cMessageReader for S2CMessageReaderTestWrapper<F, I>
where
    F: FnMut() -> usize + Send,
    I: Fn(&S2cMessage) + Send,
{
    async fn read_next_message(
        &mut self,
        reader: &mut BufReader<OwnedReadHalf>,
    ) -> Result<S2cMessage, NetworkError> {
        loop {
            let msg = self.inner.read_next_message(reader).await?;

            (self.message_inspector)(&msg);

            self.reading_counter += 1;

            let fail_every = (self.fail_every)();

            let should_fail = fail_every > 0 && self.reading_counter % fail_every == 0;

            if !should_fail {
                return Ok(msg);
            }

            if let Some(failure) = self.failures.pop_front() {
                self.failures.push_back(failure);
                match failure {
                    Failure::Io => {
                        return Err(NetworkError::Io {
                            err: "Error".to_string(),
                        });
                    }
                    Failure::Drop => {
                        continue;
                    }
                }
            } else {
                return Ok(msg);
            }
        }
    }
}
