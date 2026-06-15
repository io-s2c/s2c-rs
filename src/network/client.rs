use crate::config::S2cOptions;
use crate::context::Context;
use crate::network::client::ClientState::{Closed, Connected, NotConnected, Ready};
use crate::network::error::{ClientError, NetworkError};
use crate::network::inflight::InflightRequests;
use crate::network::message_rw::{write_message, S2cMessageReader};
use crate::proto::io::s2c_message::{Body, Error};
use crate::proto::io::{Handshake, NodeIdentity, S2cMessage};
use prost::Message;
use std::cmp::PartialEq;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncWriteExt, BufReader, BufWriter};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::{Mutex, RwLock};
use tokio_util::task::TaskTracker;
use uuid::Uuid;

#[derive(Eq, PartialEq, Debug)]
#[repr(u8)]
enum ClientState {
    NotConnected,
    Connected,
    Ready,
    Closed,
}

struct AtomicClientState(AtomicU8);

impl AtomicClientState {
    fn new() -> Self {
        Self(AtomicU8::new(NotConnected as u8))
    }

    fn load(&self) -> ClientState {
        match self.0.load(Ordering::Acquire) {
            0 => NotConnected,
            1 => Connected,
            2 => Ready,
            _ => Closed,
        }
    }

    fn store(&self, client_state: ClientState) {
        self.0.store(client_state as u8, Ordering::Release);
    }
}

struct S2cClient<T, F>
where
    T: S2cMessageReader + Send + 'static,
    F: Fn() -> T,
{
    s2c_options: &'static S2cOptions,
    context: &'static Context,
    inflight_requests: Arc<InflightRequests>,
    message_reader_factory: F,
    throttle: Arc<AtomicBool>,
    client_state: Arc<AtomicClientState>,
    sender: Arc<Sender<Option<S2cMessage>>>,
    // We use mutex because receiver needs to be mutable
    receiver: Arc<Mutex<Receiver<Option<S2cMessage>>>>,
    rw_lock: RwLock<()>,
    task_tracker: TaskTracker,
}

impl<T, F> S2cClient<T, F>
where
    T: S2cMessageReader + Send + 'static,
    F: Fn() -> T,
{
    pub fn new(
        s2c_options: &'static S2cOptions,
        context: &'static Context,
        message_reader_factory: F,
        inflight_requests: InflightRequests,
    ) -> Self {
        let (sender, receiver) = channel(s2c_options.network.max_pending_reqs_per_client as usize);
        Self {
            s2c_options,
            context,
            inflight_requests: Arc::new(inflight_requests),
            message_reader_factory,
            throttle: Arc::new(AtomicBool::new(false)),
            client_state: Arc::new(AtomicClientState::new()),
            sender: Arc::new(sender),
            receiver: Arc::new(Mutex::new(receiver)),
            rw_lock: RwLock::new(()),
            task_tracker: TaskTracker::new(),
        }
    }

    pub async fn connect(&self, server_node_id: &NodeIdentity) -> Result<(), ClientError> {
        let _guard = self.rw_lock.write().await;
        if self.client_state.load() == NotConnected {
            // Ensure drainers are closed from last run
            if self.task_tracker.close() {
                self.task_tracker.wait().await;
            }

            self.task_tracker.reopen();

            let addr = format!("{}:{}", server_node_id.address, server_node_id.port);
            let tcp_stream = tokio::time::timeout(
                tokio::time::Duration::from_millis(self.s2c_options.network.connect_timeout_ms),
                TcpStream::connect(addr),
            )
            .await
            .map_err(|_| ClientError::ConnectTimeout)?
            .map_err(|e| ClientError::Io(e.to_string()))?;

            self.client_state.store(Connected);

            let (reader, writer) = tcp_stream.into_split();

            let buf_reader = BufReader::new(reader);
            let buf_writer = BufWriter::new(writer);

            self.spawn_drain_in(buf_reader);
            self.spawn_drain_out(buf_writer);

            match self.handshake().await {
                Ok(handshake) => {
                    self.client_state.store(Ready);
                    Ok(())
                }
                // Reset
                Err(e) => {
                    self.client_state.store(NotConnected);

                    if let Err(err) = self.sender.send(None).await {
                        tracing::debug!("Error while sending {:?}", err)
                    };
                    if self.task_tracker.close() {
                        self.task_tracker.wait().await;
                    }
                    Err(e)
                }
            }
        } else {
            tracing::warn!("Wrong state for connect {:?}", self.client_state.load());
            Ok(()) // Wrong state, but not error
        }
    }

    pub async fn send(&self, message: S2cMessage, timeout: u64) -> Result<S2cMessage, ClientError> {
        {
            let _guard = self.rw_lock.read().await;
            let state = self.client_state.load();
            if state == Closed {
                return Err(ClientError::Closed);
            }
            if state == NotConnected {
                return Err(ClientError::NotConnected);
            }
        }
        self.send_and_await_response(message, timeout).await
    }

    fn spawn_drain_in(&self, buf_reader: BufReader<OwnedReadHalf>) {
        let client_state = self.client_state.clone();
        let message_reader = (self.message_reader_factory)();
        let inflight_requests = self.inflight_requests.clone();
        let sender = self.sender.clone();
        let throttle = self.throttle.clone();
        self.task_tracker.spawn(async move {
            Self::drain_in(
                buf_reader,
                client_state,
                message_reader,
                inflight_requests,
                sender,
                throttle,
            )
            .await
        });
    }

    fn spawn_drain_out(&self, buf_writer: BufWriter<OwnedWriteHalf>) {
        let client_state = self.client_state.clone();
        let receiver = self.receiver.clone();
        let throttle = self.throttle.clone();
        let s2c_options = self.s2c_options;
        self.task_tracker.spawn(async move {
            Self::drain_out(buf_writer, receiver, client_state, throttle, s2c_options).await;
        });
    }

    async fn send_and_await_response(
        &self,
        msg: S2cMessage,
        timeout_ms: u64,
    ) -> Result<S2cMessage, ClientError> {
        let correlation_id = msg.correlation_id.clone();
        let receiver = self.inflight_requests.add(correlation_id.clone()).await;

        if let Err(_) = self.sender.send(Some(msg)).await {
            return Err(ClientError::NotConnected);
        }

        match tokio::time::timeout(std::time::Duration::from_millis(timeout_ms), receiver).await {
            Ok(Ok(Ok(response))) => Ok(response),
            Ok(Ok(Err(e))) => {
                self.inflight_requests.discard(correlation_id).await;
                Err(e)
            }
            Ok(Err(_)) => {
                self.inflight_requests.discard(correlation_id).await;
                Err(ClientError::NotConnected)
            }
            Err(_) => {
                self.inflight_requests.discard(correlation_id).await;
                Err(ClientError::Timeout)
            }
        }
    }

    async fn handshake(&self) -> Result<Handshake, ClientError> {
        let message = S2cMessage {
            correlation_id: Uuid::new_v4().to_string(),
            body: Some(Body::Handshake(Handshake {
                group_id: self.context.s2c_group_id().to_string(),
                node_identity: Some(self.context.node_identity().clone()),
            })),
            error: None,
        };

        match self
            .send_and_await_response(message, self.s2c_options.network.handshake_timeout_ms)
            .await
        {
            Ok(resp) => {
                if let (Some(Body::Handshake(handshake))) = resp.body {
                    // We don't need to check the body because we wait on the correlation_id
                    Ok(handshake)
                } else {
                    // This should never happen.
                    panic!("Invalid response");
                }
            }
            Err(err) => Err(err),
        }
    }

    async fn drain_in(
        mut reader: BufReader<OwnedReadHalf>,
        client_state: Arc<AtomicClientState>,
        mut message_reader: T,
        inflight_requests: Arc<InflightRequests>,
        sender: Arc<Sender<Option<S2cMessage>>>,
        throttle: Arc<AtomicBool>,
    ) {
        loop {
            if !running(&client_state) {
                return;
            }

            match message_reader.read_next_message(&mut reader).await {
                Ok(message) => {
                    if !running(&client_state) {
                        return;
                    }
                    if let Some(error) = message.error {
                        if matches!(error, Error::SlowDownError(_)) {
                            throttle.store(true, Ordering::Release);
                        }
                    } else {
                        inflight_requests.respond(message).await;
                    }
                }
                Err(e) => {
                    tracing::error!("Error while reading message {:?}", e.to_string());
                    inflight_requests.fail_all(&ClientError::Io(e.to_string())).await;
                    if let Err(err) = sender.send(None).await {
                        tracing::debug!("Error while sending {:?}", err);
                    }
                    client_state.store(NotConnected);
                    break;
                }
            }
        }
    }

    async fn drain_out(
        mut writer: BufWriter<OwnedWriteHalf>,
        receiver: Arc<Mutex<Receiver<Option<S2cMessage>>>>,
        client_state: Arc<AtomicClientState>,
        throttle: Arc<AtomicBool>,
        s2c_options: &S2cOptions,
    ) {
        let mut receiver = receiver.lock().await;
        let mut out_buf: Vec<u8> = vec![0u8; 1024];
        loop {
            if !running(&client_state) {
                return;
            }
            if throttle.load(Ordering::Acquire) {
                tokio::time::sleep(Duration::from_millis(s2c_options.network.throttle_delay_ms))
                    .await;
                throttle.store(false, Ordering::Release);
            }
            match receiver.recv().await {
                Some(Some(out_msg)) => {
                    if !running(&client_state) {
                        return;
                    }
                    if let Err(e) = write_message(&mut writer, out_msg, &mut out_buf).await {
                        client_state.store(NotConnected);
                        break; // Writer dropped, reader clears up
                    }
                }
                _ => break,
            }
        }
    }

    pub async fn close(&self) {
        let _guard = &self.rw_lock.write().await;
        if self.client_state.load() != Closed {
            self.client_state.store(Closed);
            if let Err(err) = self.sender.send(None).await {
                tracing::debug!("Error while sending {:?}", err);
            }
            if self.task_tracker.close() {
                self.task_tracker.wait().await;
            }
        } else {
            tracing::warn!("Wrong state for close {:?}", self.client_state.load());
        }
    }
}

fn running(s: &Arc<AtomicClientState>) -> bool {
    matches!(s.load(), Ready | Connected)
}

