use crate::backoff::BackoffCounter;
use crate::config::{S2cOptions, S2cRetryOptions};
use crate::network::error::ServerError;
use crate::network::group_server::GroupServer;
use crate::network::message_rw::{S2cMessageReader, write_bytes};
use crate::network::server::ServerState::{Binding, Closed, NotBound, Ready};
use crate::proto::io::s2c_message::Body;
use crate::proto::io::{Handshake, NodeIdentity, S2cMessage};
use std::collections::HashMap;
use std::fmt;

use crate::fsm::MessageReaderFactory;
use crate::network::client::MessageReader;
use prost::Message;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Duration;
use tokio::io::{AsyncWriteExt, BufReader, BufWriter};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tokio::time;
use tokio_util::bytes::Bytes;
use tokio_util::future::FutureExt;
use tokio_util::sync::CancellationToken;
use tokio_util::task::{JoinMap, TaskTracker};

#[derive(Clone, Debug)]
#[repr(u8)]
pub enum ServerState {
    NotBound,
    Binding,
    Ready,
    Closed,
}

impl fmt::Display for ServerState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            NotBound => "NotBound",
            Binding => "Binding",
            Ready => "Ready",
            Closed => "Closed",
        };
        write!(f, "{}", s)
    }
}

#[derive(Debug)]
struct AtomicServerState(AtomicU8);

impl AtomicServerState {
    pub fn new() -> Self {
        Self(AtomicU8::new(NotBound as u8))
    }

    pub fn load(&self) -> ServerState {
        match self.0.load(Ordering::Acquire) {
            0 => NotBound,
            1 => Binding,
            2 => Ready,
            _ => Closed,
        }
    }

    pub fn store(&self, server_state: ServerState) {
        self.0.store(server_state as u8, Ordering::Release)
    }

    pub fn compare_and_exchange(&self, current: ServerState, new: ServerState) -> bool {
        self.0
            .compare_exchange(
                current as u8,
                new as u8,
                Ordering::AcqRel,
                Ordering::Relaxed,
            )
            .is_ok()
    }
}

type GroupServers = Arc<Mutex<HashMap<String, GroupServer>>>;

pub struct S2cServer {
    s2c_options: &'static S2cOptions,
    group_servers: GroupServers,
    server_state: Arc<AtomicServerState>,
    node_identity: NodeIdentity,
    message_reader_factory: MessageReaderFactory,
    join_set: JoinSet<()>,
    cancellation_token: CancellationToken,
}

impl S2cServer {
    pub fn new(
        s2c_options: &'static S2cOptions,
        node_identity: NodeIdentity,
        message_reader_factory: MessageReaderFactory,
    ) -> Self {
        Self {
            s2c_options,
            group_servers: Arc::new(Mutex::new(HashMap::new())),
            server_state: Arc::new(AtomicServerState::new()),
            node_identity,
            message_reader_factory,
            join_set: JoinSet::new(),
            cancellation_token: CancellationToken::new(),
        }
    }

    pub async fn start(&mut self) -> Result<(), ServerError> {
        if self.server_state.compare_and_exchange(NotBound, Binding) {
            let server_state = self.server_state.clone();
            let port = self.node_identity.port;
            let s2c_options = self.s2c_options;
            let tcp_listener = bind(self.node_identity.port, &self.s2c_options.retry)
                .await
                .map_err(|e| ServerError::NetworkError {
                    err: super::error::NetworkError::Io { err: e.into() },
                })?;
            let message_reader_factory = self.message_reader_factory.clone();
            let group_servers = self.group_servers.clone();
            let cancellation_token = self.cancellation_token.clone();
            server_state.store(Ready);

            Self::serve(
                server_state,
                s2c_options,
                port,
                tcp_listener,
                message_reader_factory,
                cancellation_token,
                group_servers,
            )
            .await
        } else {
            tracing::warn!("Wrong state for starting server {:?}", self.server_state);
            Err(ServerError::WrongState {
                server_state: self.server_state.load(),
            })
        }
    }

    async fn serve(
        server_state: Arc<AtomicServerState>,
        s2c_options: &'static S2cOptions,
        port: i32,
        tcp_listener: TcpListener,
        message_reader_factory: MessageReaderFactory,
        cancellation_token: CancellationToken,
        group_servers: GroupServers,
    ) -> Result<(), ServerError> {
        let mut inner_join_set: JoinSet<()> = JoinSet::new();
        tokio::select! {
            _ = cancellation_token.cancelled() => {
                inner_join_set.shutdown().await;
                Ok(())
            }
            re = Self::do_serve(server_state, s2c_options, port, &mut inner_join_set, tcp_listener, message_reader_factory, group_servers) => {
                return re;
            }
        }
    }

    async fn do_serve(
        server_state: Arc<AtomicServerState>,
        s2c_options: &'static S2cOptions,
        port: i32,
        join_set: &mut JoinSet<()>,
        tcp_listener: TcpListener,
        message_reader_factory: MessageReaderFactory,
        group_servers: GroupServers,
    ) -> Result<(), ServerError> {
        let mut tcp_listener = tcp_listener;
        loop {
            start_accept_loop(
                &tcp_listener,
                &*server_state,
                join_set,
                &message_reader_factory,
                s2c_options,
                group_servers.clone(),
            )
            .await;
            if !server_state.compare_and_exchange(Ready, NotBound) {
                return Ok(());
            }
            if server_state.compare_and_exchange(NotBound, Binding) {
                match bind(port, &s2c_options.retry).await {
                    Ok(listener) => {
                        tcp_listener = listener;
                        server_state.store(Ready);
                    }
                    Err(err) => {
                        tracing::error!("Error while rebinding {}. Closing client", err);
                        server_state.store(Closed);

                        return Err(ServerError::NetworkError {
                            err: crate::network::error::NetworkError::Io { err: err.into() },
                        });
                    }
                }
            } else {
                tracing::debug!("Couldn't rebind {:?}", server_state);
                return Err(ServerError::WrongState {
                    server_state: server_state.load(),
                });
            }
        }
    }

    pub async fn register_group_server(&self, group_id: String, group_server: GroupServer) {
        self.group_servers
            .lock()
            .await
            .insert(group_id, group_server);
    }

    pub async fn close(&mut self) {
        if matches!(self.server_state.load(), Closed) {
            tracing::warn!("Server already closed");
            return;
        }
        self.server_state.store(Closed);
        self.join_set.shutdown().await;
    }
}

async fn bind(
    port: i32,
    s2c_retry_options: &S2cRetryOptions,
) -> Result<TcpListener, std::io::Error> {
    let mut backoff = BackoffCounter::new(&s2c_retry_options);
    tracing::debug!("Server binding");
    loop {
        match TcpListener::bind(format!("0.0.0.0:{}", port)).await {
            Ok(listener) => return Ok(listener),
            Err(err) => {
                if backoff.can_attempt() {
                    backoff.await_attempt().await;
                } else {
                    return Err(err);
                }
            }
        }
    }
}
async fn start_accept_loop(
    tcp_listener: &TcpListener,
    server_state: &AtomicServerState,
    join_set: &mut JoinSet<()>,
    message_reader_factory: &MessageReaderFactory,
    s2c_options: &'static S2cOptions,
    group_servers: GroupServers,
) {
    loop {
        if matches!(server_state.load(), Closed) {
            return;
        }
        let re = tcp_listener.accept().await;
        if matches!(server_state.load(), Closed) {
            return;
        }
        match re {
            Ok((tcp_stream, sock_addr)) => {
                let message_reader_factory = message_reader_factory.clone();
                let group_servers = group_servers.clone();
                join_set.spawn(async move {
                    handle_client(
                        tcp_stream,
                        sock_addr,
                        message_reader_factory,
                        s2c_options,
                        group_servers,
                    )
                    .await;
                });
            }
            Err(err) => {
                tracing::error!("Error accepting connection {}. Exiting accept loop", err);
                return;
            }
        }
    }
}

async fn handle_client(
    tcp_stream: TcpStream,
    sock_addr: SocketAddr,
    message_reader_factory: MessageReaderFactory,
    s2c_options: &S2cOptions,
    group_servers: GroupServers,
) {
    let (read, write) = tcp_stream.into_split();
    let mut reader = BufReader::new(read);
    let mut writer = BufWriter::new(write);
    if let Some(handshake) = handshake(
        &mut reader,
        message_reader_factory(),
        &mut writer,
        s2c_options.network.handshake_timeout_ms as u64,
    )
    .await
    {
        match group_servers.lock().await.get(&handshake.group_id) {
            Some(group_server) => {
                if let Some(node_identity) = handshake.node_identity {
                    let _ = group_server.handle_client(node_identity, reader, writer);
                } else {
                    tracing::error!("Invalid handshake missing NodeIdentity")
                }
            }
            None => {
                tracing::error!("Invalid handshake with unknown group")
            }
        }
    } else {
        tracing::debug!("Couldn't handshake client. Dropping connection");
    }
}

async fn write(
    writer: &mut BufWriter<OwnedWriteHalf>,
    msg: S2cMessage,
) -> Result<(), std::io::Error> {
    write_bytes(writer, Bytes::from(msg.encode_to_vec())).await?;
    writer.flush().await
}

async fn handshake(
    reader: &mut BufReader<OwnedReadHalf>,
    mut message_reader: MessageReader,
    writer: &mut BufWriter<OwnedWriteHalf>,
    timeout: u64,
) -> Option<Handshake> {
    loop {
        match time::timeout(
            Duration::from_millis(timeout),
            message_reader.read_next_message(reader),
        )
        .await
        {
            Ok(Ok(mut msg)) => {
                let msg2 = msg.clone();
                if let Some(Body::Handshake(handshake)) = msg.body.take() {
                    tracing::debug!("Handshake received: {:?}", handshake);
                    return match write(writer, msg2).await {
                        Ok(_) => {
                            tracing::debug!("Handshake sent");
                            Some(handshake)
                        }
                        Err(_) => {
                            tracing::debug!("Failed sending handshake");
                            None
                        }
                    };
                } else {
                    tracing::warn!("Skipping unexpected non-handshake message {:?}", msg);
                }
            }
            Ok(Err(e)) => {
                tracing::error!("Error receiving handshake message: {}", e);
                return None;
            }
            Err(elapsed) => {
                tracing::debug!("Handshake timed out: {:?}", elapsed);
                return None;
            }
        }
    }
}
